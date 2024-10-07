package com.pg.replication.consumer.store;

import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.concurrent.ConcurrentSkipListSet;

@Service
public class InMemoryPartitionAssignmentStore {
    private final ConcurrentSkipListSet<Integer> masterPartitions = new ConcurrentSkipListSet<>();
    private final ConcurrentSkipListSet<Integer> replicaPartitions = new ConcurrentSkipListSet<>();

    public void addMasterPartitions(Collection<Integer> masterPartitions) {
        this.masterPartitions.addAll(masterPartitions);
    }

    public void addReplicaPartitions(Collection<Integer> replicaPartitions) {
        this.replicaPartitions.addAll(replicaPartitions);
    }

    public void removeMasterPartitions(Collection<Integer> masterPartitions) {
        this.masterPartitions.removeAll(masterPartitions);
    }

    public void removeReplicaPartitions(Collection<Integer> replicaPartitions) {
        this.replicaPartitions.removeAll(replicaPartitions);
    }

    public boolean isAssignmentPresent() {
        return !masterPartitions.isEmpty() || !replicaPartitions.isEmpty();
    }
}
