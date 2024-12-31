


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
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

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
		DependencyMiner.TaskDetails taskDetails;

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
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {
		int fileId1 = message.getTaskDetails().fileId1;
		Set<String> file1Columns = message.getTaskDetails().file1Columns;
		String file1ColumnHeader=message.getTaskDetails().File1ColumnHeader;
		int fileId2 = message.getTaskDetails().fileId2;
		String file2ColumnHeader=message.getTaskDetails().File2ColumnHeader;
		Set<String> file2Columns = message.getTaskDetails().file2Columns;


		// Process the columns of both files to find Inclusion Dependencies (INDs)
		List<InclusionDependency> discoveredInds = discoverInds(fileId1, file1ColumnHeader,file1Columns, fileId2, file2ColumnHeader,file2Columns);

		int result=discoveredInds.size();
		// Create the CompletionMessage with the necessary details
		DependencyMiner.CompletionMessage completionMessage = new DependencyMiner.CompletionMessage(
				this.getContext().getSelf(),   // The worker's reference (actor ID)
				result                          // The result (e.g., IND count or any other data)
		);

		// Send the CompletionMessage back to DependencyMiner
		message.getDependencyMinerLargeMessageProxy().tell(
				new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy())
		);
		return this;

	}
	static List<InclusionDependency> inds = new ArrayList<>();
	private List<InclusionDependency> discoverInds(int fileId1,String file1ColumnHeader,Set<String> file1Columns,
												   int fileId2,String file2ColumnHeader,Set<String> file2Columns) {


		List<InclusionDependency> single_ind = new ArrayList<>();
		if (file1Columns == null || file2Columns == null) {
			this.getContext().getLog().error("Columns for file " + fileId1 + " or " + fileId2 + " are null!");
			return inds; // Return empty list if columns are null
		}

		// Check if column1's values are a subset of column2's values (this is the IND condition)
		if (file1Columns.containsAll(file2Columns)) {
			InclusionDependency ind = new InclusionDependency(
					new File("file" + fileId1),  // Example, replace with actual file information
					new String[] {file1ColumnHeader},
					new File("file" + fileId2),
					new String[] {file2ColumnHeader}
			);

			// Add the discovered IND to the list
			inds.add(ind);
			single_ind.add(ind);
			// Optionally log the discovered IND
			this.getContext().getLog().info(
					"IND Found: " + " (File " + fileId1 + " "+file1ColumnHeader +") ⊆ " +
							" (File " + fileId2+ " "+file2ColumnHeader +" Total IND" +inds.size());

		}

		return single_ind;
	}

}


