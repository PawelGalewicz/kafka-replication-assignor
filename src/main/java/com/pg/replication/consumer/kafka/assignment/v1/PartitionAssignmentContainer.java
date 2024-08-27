package com.pg.replication.consumer.kafka.assignment.v1;

import lombok.Getter;
import org.apache.kafka.common.TopicPartition;

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
//        fixme maybe the size can be estimated
        masterPartitionToInstanceAssignment = new HashMap<>();
        replicaPartitionToInstanceAssignment = new HashMap<>();
    }

    public void addReplicaAssignment(TopicPartition topicPartition, String instance) {
        replicaPartitionToInstanceAssignment.put(topicPartition.partition(), instance);
        replicaPartitionsToAssign.clear(topicPartition.partition());
    }

    public void addMasterAssignment(TopicPartition topicPartition, String instance) {
        masterPartitionToInstanceAssignment.put(topicPartition.partition(), instance);
        masterPartitionsToAssign.clear(topicPartition.partition());
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
//        fixme maybe we won't need that later
//        we don't update replicaPartitionsToAssign here as we don't want to assign them straight away, but rather using incremental rebalance
    }

    public void removeMasterPartition(TopicPartition masterPartition) {
        int partition = masterPartition.partition();
        masterPartitionToInstanceAssignment.remove(partition);
        masterPartitionsToAssign.set(partition);
    }


    public void removeReplicaPartition(TopicPartition replicaPartition) {
        int partition = replicaPartition.partition();
        replicaPartitionToInstanceAssignment.remove(partition);
        replicaPartitionsToAssign.set(partition);
    }
}
