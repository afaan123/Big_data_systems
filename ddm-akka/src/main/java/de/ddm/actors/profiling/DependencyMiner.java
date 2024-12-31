
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
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import de.ddm.utils.MemoryUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.File;
import java.io.FileNotFoundException;
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

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

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
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

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
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10000));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		return this;
	}

	private final Map<Integer, Map<String, Set<String>>> columnValues = new HashMap<>();
	private Map<Integer, Boolean> fileCompletionFlags = new HashMap<>();  // Keep track of file completion

	static int inputReaderCounter = 0;
	private Behavior<Message> handle(BatchMessage message) {


		List<String[]> batch = message.getBatch();
		int fileId = message.getId();

		// Ensure columnValues map is initialized for the file
		columnValues.putIfAbsent(fileId, new HashMap<>());

		// Process each row in the batch
		for (String[] row : batch) {
			for (int colIndex = 0; colIndex < row.length; colIndex++) {
				String columnName = "Column_" + colIndex;
				columnValues.get(fileId)
						.computeIfAbsent(columnName, k -> new HashSet<>())
						.add(row[colIndex]);
			}
		}
		// If the batch is empty, this could be the end of data for this file
		if (batch.isEmpty()) {
			// If there is no more data, we set a completion flag for the file
			fileCompletionFlags.put(fileId, true);
			inputReaderCounter++;
		}
		this.getContext().getLog().info("InputReader"+inputReaders.size()+"Count"+inputReaderCounter+"file"+inputFiles.length);
		if(inputReaderCounter == inputFiles.length) {
			distributeWorkToWorkers();
		}


		if (!message.getBatch().isEmpty())
			this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10000));
		else {
			this.getContext().getLog().info("All batches processed for InputReader " + message.getId());
		}

		return this;

	}


	private int count=0;
	private void distributeWorkToWorkers() {
		if(workerAvailable){
		List<ActorRef<DependencyWorker.Message>> workers = new ArrayList<>();

		for (Integer fileId1 : columnValues.keySet()) {
			Map<String, Set<String>> dependentColumns = columnValues.get(fileId1);

			// Self-comparison within the same file
			for (Map.Entry<String, Set<String>> dependentEntry : dependentColumns.entrySet()) {
				for (Map.Entry<String, Set<String>> referencedEntry : dependentColumns.entrySet()) {
					if (dependentEntry.getKey().equals(referencedEntry.getKey())) continue;

					// Unique worker name for self-comparison
					String workerName = "dependencyWorker-" + fileId1 + "-self-" + count++;
					ActorRef<DependencyWorker.Message> worker = this.getContext().spawn(
							DependencyWorker.create(),
							workerName
					);

					workers.add(worker);
					workerTaskDetails.put(worker, new TaskDetails(
							fileId1, dependentEntry.getKey(), dependentEntry.getValue(),
							fileId1, referencedEntry.getKey(), referencedEntry.getValue()
					));

					// Create a TaskMessage for this combination
					DependencyWorker.TaskMessage taskMessage = new DependencyWorker.TaskMessage(
							this.largeMessageProxy,
							new TaskDetails(
									fileId1, dependentEntry.getKey(), dependentEntry.getValue(),
									fileId1, referencedEntry.getKey(), referencedEntry.getValue()
							)
					);
					// Send the TaskMessage to the worker
					if (dependentEntry.getValue() == null || dependentEntry.getValue().isEmpty() ||
							referencedEntry.getValue() == null || referencedEntry.getValue().isEmpty())
						this.getContext().getLog().info("Worker " + workerName + " is empty");
					String taskKey = fileId1 + "-" + dependentEntry.getKey() + "-" + fileId1 + "-" + referencedEntry.getKey();
					this.getContext().getLog().info(taskKey + " " + worker);

					worker.tell(taskMessage);
				}
			}

			// Comparison across different files
			for (Map.Entry<Integer, Map<String, Set<String>>> otherFileEntry : columnValues.entrySet()) {
				int otherFileId = otherFileEntry.getKey();
				if (otherFileId == fileId1) continue;

				Map<String, Set<String>> referencedColumns = otherFileEntry.getValue();

				for (Map.Entry<String, Set<String>> dependentEntry : dependentColumns.entrySet()) {
					for (Map.Entry<String, Set<String>> referencedEntry : referencedColumns.entrySet()) {

						// Unique worker name for comparison across files
						String workerName = "dependencyWorker-" + fileId1 + "-" + otherFileId + "-cross-" + count++;
						ActorRef<DependencyWorker.Message> worker = this.getContext().spawn(
								DependencyWorker.create(),
								workerName
						);
						workers.add(worker);
						workerTaskDetails.put(worker, new TaskDetails(
								fileId1, dependentEntry.getKey(), dependentEntry.getValue(),
								otherFileId, referencedEntry.getKey(), referencedEntry.getValue()
						));

						// Create a TaskMessage for this combination
						DependencyWorker.TaskMessage taskMessage = new DependencyWorker.TaskMessage(
								this.largeMessageProxy,  // The reference to the LargeMessageProxy actor
								new TaskDetails(
										fileId1, dependentEntry.getKey(), dependentEntry.getValue(),
										otherFileId, referencedEntry.getKey(), referencedEntry.getValue()
								)   // Second file ID and its columns
						);

						// Send the TaskMessage to the worker
						if (dependentEntry.getValue() == null || dependentEntry.getValue().isEmpty() ||
								referencedEntry.getValue() == null || referencedEntry.getValue().isEmpty())
							this.getContext().getLog().info("Worker " + workerName + " is empty");
						String taskKey = fileId1 + "-" + dependentEntry.getKey() + "-" + otherFileId + "-" + referencedEntry.getKey();
						this.getContext().getLog().info(taskKey + " Creating " + worker);

						worker.tell(taskMessage);
					}
				}
			}
		}
		}else {
			this.getContext().getLog().info("No Worker. Waiting for active workers");
		}
	}


	private boolean workerAvailable=false;
	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);
			if(!workerAvailable) {
				workerAvailable = true;
				distributeWorkToWorkers();
			}
			// The worker should get some work ... let me send her something before I figure out what I actually want from her.
			// I probably need to idle the worker for a while, if I do not have work for it right now ... (see master/worker pattern)
			//this.getContext().getLog().info("Registered dependency worker " + dependencyWorker);
			//dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, 42,null,42,null));
		}
		return this;


	}

	private static int ind_Count=0;
	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		int result = message.getResult();

		// Retrieve task details associated with the worker (dependencyWorker)
		TaskDetails taskDetails = workerTaskDetails.get(dependencyWorker);

		if (taskDetails != null) {
			// Process the task details and result
			int fileId1 = taskDetails.fileId1;
			String file1ColumnHeader= taskDetails.File1ColumnHeader;
			Set<String> file1Columns = taskDetails.file1Columns;
			int fileId2 = taskDetails.fileId2;
			String file2ColumnHeader= taskDetails.File2ColumnHeader;
			Set<String> file2Columns = taskDetails.file2Columns;

			// Log or process the result
			if(result==1) {
				ind_Count++;
				this.getContext().getLog().info(
						"IND Status: " + " (F " + fileId1 +" "+ file1ColumnHeader+ ") ⊆ " +
								" (F" + fileId2 + " "+ file2ColumnHeader+" Total "   +" count " + ind_Count);
			}
			// Optionally, you can perform further processing, aggregate results, or notify other actors
		} else {
			this.getContext().getLog().error("No task details found for worker: " + dependencyWorker);
		}

		// Remove the worker from the task details map after processing
		workerTaskDetails.remove(dependencyWorker);
		return this;

	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dependencyWorkers.remove(dependencyWorker);
		return this;
	}

	private final Map<ActorRef<DependencyWorker.Message>, TaskDetails> workerTaskDetails = new HashMap<>();

	// TaskDetails class to hold the file and column data for each task


	@Getter @Setter
	@AllArgsConstructor
	public static class TaskDetails {
		int fileId1;
		String File1ColumnHeader;
		Set<String> file1Columns;
		int fileId2;
		String File2ColumnHeader;
		Set<String> file2Columns;
	}

}

