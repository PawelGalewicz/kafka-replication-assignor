package com.pg.replication.consumer.kafka.assignor;

import lombok.Getter;

import java.util.*;

public class PartitionAssignmentContainer {
    @Getter
    private final Map<Integer, String> masterPartitionToInstanceAssignment;
    @Getter
    private final Map<Integer, String> replicaPartitionToInstanceAssignment;
    @Getter
    private final BitSet masterPartitionsToAssign;
    @Getter
    private final BitSet replicaPartitionsToAssign;

    public PartitionAssignmentContainer(Integer masterPartitionsCount, Integer replicaPartitionsCount) {
        masterPartitionsToAssign = new BitSet(masterPartitionsCount);
        masterPartitionsToAssign.set(0, masterPartitionsCount);
        replicaPartitionsToAssign = new BitSet(replicaPartitionsCount);
        replicaPartitionsToAssign.set(0, replicaPartitionsCount);
        masterPartitionToInstanceAssignment = new HashMap<>(masterPartitionsCount);
        replicaPartitionToInstanceAssignment = new HashMap<>(replicaPartitionsCount);
    }

    public void addReplicaAssignment(String instance, int replicaPartition) {
        replicaPartitionToInstanceAssignment.put(replicaPartition, instance);
        replicaPartitionsToAssign.clear(replicaPartition);
    }

    public void addMasterAssignment(String instance, int masterPartition) {
        masterPartitionToInstanceAssignment.put(masterPartition, instance);
        masterPartitionsToAssign.clear(masterPartition);
    }

    public Optional<String> getReplicaInstanceForPartition(Integer partition) {
        return Optional.ofNullable(replicaPartitionToInstanceAssignment.get(partition));
    }

    public Optional<String> getMasterInstanceForPartition(Integer partition) {
        return Optional.ofNullable(masterPartitionToInstanceAssignment.get(partition));
    }

    public void promoteReplicaToMaster(String instance, Integer partition) {
        replicaPartitionToInstanceAssignment.remove(partition);
        masterPartitionToInstanceAssignment.put(partition, instance);
        masterPartitionsToAssign.set(partition);
    }

    public void removeMasterPartition(Integer masterPartition) {
        masterPartitionToInstanceAssignment.remove(masterPartition);
        masterPartitionsToAssign.set(masterPartition);
    }

    public void removeReplicaPartition(Integer partition) {
        replicaPartitionToInstanceAssignment.remove(partition);
        replicaPartitionsToAssign.set(partition);
    }

    public boolean hasPendingAssignments() {
        return replicaPartitionsToAssign.cardinality() + masterPartitionsToAssign.cardinality() > 0;
    }
}
