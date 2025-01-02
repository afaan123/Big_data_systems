

package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.*;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		int task;
	}

	@Getter
	@AllArgsConstructor
	public static class TaskDetails implements DependencyWorker.Message, LargeMessageProxy.LargeMessage {
		private static final long serialVersionUID = 3879890900848542869L;
		private final ActorRef<DependencyMiner.Message> dependencyMinerRef;
		int fileId1;
		String file1ColumnHeader;
		int fileId2;
		String file2ColumnHeader;
	}

	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = -4932562889190683168L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<Message> context) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.onMessage(TaskDetails.class,this::handle)
				.onMessage(DataChunkMessage.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle)
				.build();
	}

	private final Map<String, Set<String>> firstColumnData = new HashMap<>();
	private final Map<String, Set<String>> secondColumnData = new HashMap<>();

	private Behavior<Message> handle(DataChunkMessage chunkMessage) {
		if (chunkMessage.getChunkValues() == null || chunkMessage.getChunkValues().isEmpty()) {
			this.getContext().getLog().error("Received an empty or null chunk");
			return this;
		}

		int result = 0;
		ActorRef<DependencyMiner.Message> minerRef = chunkMessage.getDependencyMinerRef();
		String taskId = chunkMessage.getTaskId();
		boolean isLastChunk = chunkMessage.isLastChunk();
		boolean isFirstColumnValues = chunkMessage.isFirstColumnValues();
		Set<String> chunk = chunkMessage.getChunkValues();

		if (isFirstColumnValues) {
			firstColumnData.putIfAbsent(taskId, new HashSet<>());
			firstColumnData.get(taskId).addAll(chunk);
		} else {
			secondColumnData.putIfAbsent(taskId, new HashSet<>());
			secondColumnData.get(taskId).addAll(chunk);
		}

		if (isLastChunk) {
			// Mark task as complete if both dependent and referenced are finished
			if (firstColumnData.containsKey(taskId) && secondColumnData.containsKey(taskId)) {
				this.getContext().getLog().info("Task complete for {}. Dependent: {}, Referenced: {}", taskId, firstColumnData.size(), secondColumnData.size());
				// Call a method to process the task (e.g., discover INDs)
				Set<String> firstColumnValues = firstColumnData.get(taskId);
				Set<String> secondColumnValues = secondColumnData.get(taskId);
				result = testIND(firstColumnValues, secondColumnValues);
				DependencyMiner.CompletionMessage completionMessage = new DependencyMiner.CompletionMessage(
						this.getContext().getSelf(),
						result
				);
				minerRef.tell(completionMessage);
			}
		}
		return this;
	}

	private Behavior<Message> handle(TaskDetails taskDetails) {
		ActorRef<DependencyMiner.Message> minerActorRef = taskDetails.dependencyMinerRef;
		int fileId1 = taskDetails.getFileId1();
		String file1ColumnHeader = taskDetails.getFile1ColumnHeader();
		int fileId2 = taskDetails.getFileId2();
		String file2ColumnHeader = taskDetails.getFile2ColumnHeader();

		return this;
	}

	private static int counttestINDExecutionNo =0;
	private int testIND(Set<String> firstColumn, Set<String> secondColumn) {
		counttestINDExecutionNo++;
		int result=0;
		this.getContext().getLog().info("Processing task with dependent size {} and referenced size {} count {}",
				firstColumn.size(), secondColumn.size(),counttestINDExecutionNo);
		// Example: Check if dependent is a subset of referenced
		if (new HashSet<>(secondColumn).containsAll(firstColumn)) {
			result = 1;
			this.getContext().getLog().info("IND Found for task count {} ", counttestINDExecutionNo );
		}
		return result;
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {

        /*LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), result);
        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));
*/
		return this;
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		return Behaviors.stopped();
	}
}

