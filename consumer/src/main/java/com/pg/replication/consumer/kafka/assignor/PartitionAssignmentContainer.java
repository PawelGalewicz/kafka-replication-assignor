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
    private final BitSet replicaPartitionsToAssign;

    public PartitionAssignmentContainer(Integer masterPartitionsCount, Integer replicaPartitionsCount) {
        masterPartitionsToAssign = new BitSet(masterPartitionsCount);
        masterPartitionsToAssign.set(0, masterPartitionsCount);
        replicaPartitionsToAssign = new BitSet(replicaPartitionsCount);
        replicaPartitionsToAssign.set(0, replicaPartitionsCount);
//        fixme maybe the size can be estimated
        masterPartitionToInstanceAssignment = new HashMap<>();
        replicaPartitionToInstanceAssignment = new HashMap<>();
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
//        we don't update replicaPartitionsToAssign here as we don't want to assign them straight away, but rather using incremental rebalance
    }

    public void removeMasterPartition(Integer masterPartition) {
        masterPartitionToInstanceAssignment.remove(masterPartition);
        masterPartitionsToAssign.set(masterPartition);
    }

    public Queue<Integer> getReplicaPartitionsToAssign() {
        Queue<Integer> replicasToAssign = new LinkedList<>();

        for (int replicaPartition = replicaPartitionsToAssign.nextSetBit(0);
             replicaPartition >= 0;
             replicaPartition = replicaPartitionsToAssign.nextSetBit(replicaPartition + 1)) {
            replicasToAssign.add(replicaPartition);
        }

        return replicasToAssign;
    }

    public void removeReplicaPartition(Integer partition) {
        replicaPartitionToInstanceAssignment.remove(partition);
        replicaPartitionsToAssign.set(partition);
    }

    public boolean hasPendingAssignments() {
        return replicaPartitionsToAssign.cardinality() + masterPartitionsToAssign.cardinality() > 0;
    }
}
