package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import de.ddm.actors.patterns.LargeMessageProxy;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.Set;

@Getter
@AllArgsConstructor

public  class DataChunkMessage implements LargeMessageProxy.LargeMessage, DependencyWorker.Message, Serializable {
    private static final long serialVersionUID = -2441294145271964152L;

    private final ActorRef<DependencyMiner.Message> dependencyMinerRef;
    private String taskId;
    private Set<String> chunkValues; // Subset of column values
    private boolean isFirstColumnValues; // Indicates if this is the last chunk
    private boolean isLastChunk;
}

