
package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.Guardian;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.File;
import java.util.*;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		int result;
	}

	@Getter
	@Setter
	@AllArgsConstructor
	public static class TaskQueueMessage {
		int fileId1;
		String file1ColumnHeader;
		int fileId2;
		String file2ColumnHeader;
	}

	@Getter
	@Setter
	@AllArgsConstructor
	public static class TransferMessageAcknowledge implements DependencyMiner.Message {
		ActorRef<DependencyWorker.Message> dependencyWorker;
		int fileId1;
		String file1ColumnHeader;
		int fileId2;
		String file2ColumnHeader;
	}

	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = 7516129288777469221L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey
			.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(
					context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(
				LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new ArrayList<>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;

	private final Map<Integer, Map<String, Set<String>>> datasetColumnarValues = new HashMap<>();
	private Map<Integer, Boolean> fileCompletionFlags = new HashMap<>(); // Keep track of file completion
	private static int inputReaderCounter = 0;
	private boolean taskQueueReady = false;
	private final List<ActorRef<DependencyWorker.Message>> pendingRegistrations = new ArrayList<>();
	private final Map<Integer, Map<String, Boolean>> transferredData = new HashMap<>();
	private final Queue<TaskQueueMessage> taskQueue = new LinkedList<>();
	private final Queue<Map<Integer, List<String>>> transferDataQueue = new LinkedList<>();
	private final Map<ActorRef<DependencyWorker.Message>, Boolean> workerAvailability = new HashMap<>();
	private final Map<ActorRef<DependencyWorker.Message>, TaskQueueMessage> workerTaskDetails = new HashMap<>();
	private final Map<Integer, Map<String, Map<Integer, Set<String>>>> indGraph = new HashMap<>(); // Graph to store IND
																									// relationships
																									// file -> column ->
																									// file -> column

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onMessage(TransferMessageAcknowledge.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		return Behaviors.stopped();
	}

	private Behavior<Message> handle(TransferMessageAcknowledge transferMessageAcknowledge) {
		ActorRef<DependencyWorker.Message> worker = transferMessageAcknowledge.dependencyWorker;
		int fileId1 = transferMessageAcknowledge.getFileId1();
		String file1ColumnHeader = transferMessageAcknowledge.getFile1ColumnHeader();
		int fileId2 = transferMessageAcknowledge.getFileId2();
		String file2ColumnHeader = transferMessageAcknowledge.getFile2ColumnHeader();
		this.getContext().getLog().info("Data received by worker");
		workerAvailability.put(worker, true);
		taskQueue.add(new TaskQueueMessage(fileId1, file1ColumnHeader, fileId2, file2ColumnHeader));
		return this;
	}

	private Behavior<Message> handle(StartMessage message) {
		for (int i = 0; i < this.inputReaders.size(); i++) {
			ActorRef<InputReader.Message> inputReader = this.inputReaders.get(i);
			String fileName = inputFiles[i].getName();
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		}
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10000));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
		List<String[]> batch = message.getBatch();
		int fileId = message.getId();
		datasetColumnarValues.putIfAbsent(fileId, new HashMap<>());
		for (String[] row : batch) {
			for (int colIndex = 0; colIndex < row.length; colIndex++) {
				String columnName = "Column_" + colIndex;
				datasetColumnarValues.get(fileId)
						.computeIfAbsent(columnName, k -> new HashSet<>())
						.add(row[colIndex]);
			}
		}
		if (batch.isEmpty()) {
			fileCompletionFlags.put(fileId, true);
			inputReaderCounter++;
		}
		if (inputReaderCounter == inputFiles.length) {
			maintainTaskQueue();
		}
		if (!message.getBatch().isEmpty())
			this.inputReaders.get(message.getId())
					.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10000));
		else {
			this.getContext().getLog().info("All batches processed for InputReader " + message.getId());
		}
		return this;
	}

	private void maintainTaskQueue() {
		for (Integer fileId1 : datasetColumnarValues.keySet()) {
			Map<Integer, List<String>> dataMap = new HashMap<>();
			List<String> columnHeader = new ArrayList<>(datasetColumnarValues.get(fileId1).keySet());
			dataMap.put(fileId1, columnHeader);
			transferDataQueue.add(dataMap);

			Map<String, Set<String>> dependentColumns = datasetColumnarValues.get(fileId1);
			// Self-comparison within the same file
			for (Map.Entry<String, Set<String>> dependentEntry : dependentColumns.entrySet()) {
				for (Map.Entry<String, Set<String>> referencedEntry : dependentColumns.entrySet()) {
					if (dependentEntry.getKey().equals(referencedEntry.getKey()))
						continue;
					taskQueue.add(new TaskQueueMessage(
							fileId1, dependentEntry.getKey(),
							fileId1, referencedEntry.getKey()));
					String taskKey = fileId1 + "-" + dependentEntry.getKey() + "-" + fileId1 + "-"
							+ referencedEntry.getKey();
					// this.getContext().getLog().info( "Create Q"+taskKey);
				}
			}
			// Comparison across different files
			for (Map.Entry<Integer, Map<String, Set<String>>> otherFileEntry : datasetColumnarValues.entrySet()) {
				int otherFileId = otherFileEntry.getKey();
				if (otherFileId == fileId1)
					continue;
				Map<String, Set<String>> referencedColumns = otherFileEntry.getValue();
				for (Map.Entry<String, Set<String>> dependentEntry : dependentColumns.entrySet()) {
					for (Map.Entry<String, Set<String>> referencedEntry : referencedColumns.entrySet()) {
						taskQueue.add(new TaskQueueMessage(
								fileId1, dependentEntry.getKey(),
								otherFileId, referencedEntry.getKey()));
						String taskKey = fileId1 + "-" + dependentEntry.getKey() + "-" + otherFileId + "-"
								+ referencedEntry.getKey();
						// this.getContext().getLog().info("Create QC"+ taskKey);
					}
				}
			}
		}
		taskQueueReady = true;
		setTaskQueueReady();
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();

		if (!taskQueueReady) {
			// this.getContext().getLog().info("Task queue not ready. Queuing worker
			// registration: {}", dependencyWorker);
			pendingRegistrations.add(dependencyWorker); // Add to pending list
			return this;
		}

		// Proceed with registration if taskQueueReady is true
		registerWorker(dependencyWorker);
		return this;
	}

	private void registerWorker(ActorRef<DependencyWorker.Message> dependencyWorker) {
		if (!dependencyWorkers.contains(dependencyWorker)) {
			// this.getContext().getLog().info("Worker registered: {}", dependencyWorker);
			dependencyWorkers.add(dependencyWorker);
			workerAvailability.put(dependencyWorker, true);
			this.getContext().watch(dependencyWorker);
			assignTasksToIdleWorkers();
			// no task assignment here first will transfer the data to workers once only

		}
	}

	private void setTaskQueueReady() {
		if (taskQueueReady) {
			this.getContext().getLog().info("Task queue is now ready. Processing pending registrations...");
			for (ActorRef<DependencyWorker.Message> worker : pendingRegistrations) {
				registerWorker(worker);
			}
			pendingRegistrations.clear(); // Clear the pending list after processing
		}
	}

	private void assignTasksToIdleWorkers() {
		for (Map.Entry<ActorRef<DependencyWorker.Message>, Boolean> entry : this.workerAvailability.entrySet()) {
			if (entry.getValue() && !taskQueue.isEmpty()) {
				// this.getContext().getLog().info("Assigning task to worker: {}",
				// entry.getKey());
				assignTaskIfAvailable(entry.getKey());
			}
		}
		if (taskQueue.isEmpty() && workerTaskDetails.isEmpty()) {
			this.getContext().getLog().info("Shutting down system as taskQueue and workerTaskDetails are empty.");
			this.end();
		}
	}

	private void assignTaskIfAvailable(ActorRef<DependencyWorker.Message> worker) {
		if (!taskQueue.isEmpty()) {

			TaskQueueMessage task = taskQueue.poll();

			int fileId1 = task.getFileId1();
			String file1ColumnHeader = task.getFile1ColumnHeader();
			int fileId2 = task.getFileId2();
			String file2ColumnHeader = task.getFile2ColumnHeader();
			if (isTransitive(fileId1, file1ColumnHeader, fileId2, file2ColumnHeader)) {
				addIND(fileId1, file1ColumnHeader, fileId2, file2ColumnHeader);
				sendResultToCollector(new TaskQueueMessage(fileId1, file1ColumnHeader, fileId2, file2ColumnHeader));
				assignTasksToIdleWorkers();
			} else {

				String taskId = fileId1 + "-" + file1ColumnHeader + "to" + fileId2 + "-" + file2ColumnHeader;
				Set<String> firstColumnValues = datasetColumnarValues.get(fileId1).get(file1ColumnHeader);
				Set<String> secondColumnValues = datasetColumnarValues.get(fileId2).get(file2ColumnHeader);

				workerTaskDetails.put(worker, new TaskQueueMessage(
						task.getFileId1(), task.getFile1ColumnHeader(),
						task.getFileId2(), task.getFile2ColumnHeader()));

				int chunkSize = 1000;
				if (firstColumnValues == null || firstColumnValues.isEmpty() || secondColumnValues == null
						|| secondColumnValues.isEmpty()) {
					this.getContext().getLog().error("NDA FileID1: {}, ColumnHeader: {} FileID2: {}, ColumnHeader: {}",
							fileId1, file1ColumnHeader, fileId2, file2ColumnHeader);
					// taskQueue.add(task);
					return;
				}

				for (int i = 0; i < firstColumnValues.size(); i += chunkSize) {
					int chunkEndRange = Math.min(i + chunkSize, firstColumnValues.size());
					Set<String> chunk = new HashSet<>((new ArrayList<>(firstColumnValues)).subList(i, chunkEndRange));
					boolean isLastChunk = (i + chunkSize >= firstColumnValues.size());

					DataChunkMessage chunkMessage = new DataChunkMessage(
							this.getContext().getSelf(), taskId, chunk, true, isLastChunk);
					// Send each chunk via LargeMessageProxy
					ActorRef<LargeMessageProxy.Message> workerProxy = this.getContext().spawn(
							LargeMessageProxy.create(worker.unsafeUpcast()),
							"workerLargeMessageProxy_" + UUID.randomUUID());
					workerProxy.tell(new LargeMessageProxy.SendMessage(chunkMessage, workerProxy));
				}
				for (int i = 0; i < secondColumnValues.size(); i += chunkSize) {
					int chunkEndRange = Math.min(i + chunkSize, secondColumnValues.size());
					Set<String> chunk = new HashSet<>((new ArrayList<>(secondColumnValues)).subList(i, chunkEndRange));
					boolean isLastChunk = (i + chunkSize >= secondColumnValues.size());

					DataChunkMessage chunkMessage = new DataChunkMessage(
							this.getContext().getSelf(), taskId, chunk, false, isLastChunk);
					// Send each chunk via LargeMessageProxy
					ActorRef<LargeMessageProxy.Message> workerProxy = this.getContext().spawn(
							LargeMessageProxy.create(worker.unsafeUpcast()),
							"workerLargeMessageProxy_" + UUID.randomUUID());
					workerProxy.tell(new LargeMessageProxy.SendMessage(chunkMessage, workerProxy));
				}
				workerAvailability.put(worker, false);
			}
		}
	}

	private void sendResultToCollector(TaskQueueMessage taskPerformed) {
		int dependent = taskPerformed.getFileId1();
		int referenced = taskPerformed.getFileId2();
		String dependentColumnHeader = taskPerformed.getFile1ColumnHeader();
		String referencedColumnHeader = taskPerformed.getFile2ColumnHeader();
		int dependentColNo = Integer.parseInt(dependentColumnHeader.substring(dependentColumnHeader.indexOf("_") + 1));
		int referencedColNo = Integer
				.parseInt(referencedColumnHeader.substring(referencedColumnHeader.indexOf("_") + 1));
		File dependentFile = this.inputFiles[dependent];
		File referencedFile = this.inputFiles[referenced];
		String[] dependentAttributes = { this.headerLines[dependent][dependentColNo] };
		String[] referencedAttributes = { this.headerLines[referenced][referencedColNo] };
		InclusionDependency ind = new InclusionDependency(dependentFile, dependentAttributes, referencedFile,
				referencedAttributes);
		// InclusionDependency ind =new InclusionDependency(new File("file"), new String
		// [] {"attr"},new File("file2"),new String[] {"attr2"});

		List<InclusionDependency> inds = new ArrayList<>(1);
		inds.add(ind);
		addIND(dependent, dependentColumnHeader, referenced, referencedColumnHeader);
		this.getContext().getLog().info("INDS found");
		this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
	}

	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		TaskQueueMessage taskPerformed = workerTaskDetails.get(dependencyWorker);
		if (message.getResult() == 1) {
			sendResultToCollector(taskPerformed);
		}
		workerTaskDetails.remove(dependencyWorker);
		workerAvailability.put(dependencyWorker, true);
		assignTasksToIdleWorkers();
		return this;
	}

	private boolean isTransitive(int fileId1, String file1ColumnHeader, int fileId2, String file2ColumnHeader) {
		if (!indGraph.containsKey(fileId1) || !indGraph.get(fileId1).containsKey(file1ColumnHeader)) {
			return false;
		}
		Set<String> visited = new HashSet<>();
		Queue<String> queue = new LinkedList<>();
		queue.add(fileId1 + ":" + file1ColumnHeader);
		while (!queue.isEmpty()) {
			String current = queue.poll();
			if (current.equals(fileId2 + ":" + file2ColumnHeader)) {
				this.getContext().getLog().info("Transitive found");
				return true;
			}
			if (visited.add(current)) {
				String[] parts = current.split(":");
				int currentFile = Integer.parseInt(parts[0]);
				String currentColumn = parts[1];
				if (indGraph.containsKey(currentFile) && indGraph.get(currentFile).containsKey(currentColumn)) {
					for (Map.Entry<Integer, Set<String>> entry : indGraph.get(currentFile).get(currentColumn)
							.entrySet()) {
						int nextFile = entry.getKey();
						for (String nextColumn : entry.getValue()) {
							queue.add(nextFile + ":" + nextColumn);
						}
					}
				}
			}
		}
		return false;
	}

	private void addIND(int fileA, String columnA, int fileB, String columnB) {
		indGraph.computeIfAbsent(fileA, k -> new HashMap<>())
				.computeIfAbsent(columnA, k -> new HashMap<>())
				.computeIfAbsent(fileB, k -> new HashSet<>())
				.add(columnB);
	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dependencyWorkers.remove(dependencyWorker);

		/*
		 * if(this.busyWorkers.containsKey(dependencyWorker) &&
		 * this.busyWorkers.get(dependencyWorker) != null){
		 * this.taskKeys.add(this.busyWorkers.get(dependencyWorker));
		 * this.busyWorkers.remove(dependencyWorker);
		 * }
		 */
		return this;
	}

	// private Behavior<Message> handle(Guardian.ShutdownMessage message) {
	// this.getContext().getLog().info("Shutting down Dependency Miner!");
	// return Behaviors.stopped();
	// }

}
