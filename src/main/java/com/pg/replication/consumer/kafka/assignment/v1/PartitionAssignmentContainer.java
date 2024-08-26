package com.pg.replication.consumer.kafka.assignment.v1;

import lombok.Getter;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class PartitionAssignmentContainer {
    @Getter
    private final Map<Integer, String> masterPartitionToInstanceAssignment = new HashMap<>();
    @Getter
    private final Map<Integer, String> replicaPartitionToInstanceAssignment = new HashMap<>();

//    fixme use bitset?
    @Getter
    private final Set<Integer> masterPartitionsToAssign = new HashSet<>();
    @Getter
    private final Set<Integer> replicaPartitionsToAssign = new HashSet<>();
//    @Getter
//    private final Set<TopicPartition> otherPartitionsToAssign = new HashSet<>();

//    public void addOtherPartition(TopicPartition topicPartition) {
//        otherPartitionsToAssign.add(topicPartition);
//    }

    public void addReplicaPartition(TopicPartition topicPartition) {
        replicaPartitionsToAssign.add(topicPartition.partition());
    }

    public void addMasterPartition(TopicPartition topicPartition) {
        masterPartitionsToAssign.add(topicPartition.partition());
    }

//    public void addOtherAssignment(TopicPartition topicPartition) {
//        otherPartitionsToAssign.remove(topicPartition);
//    }

    public void addReplicaAssignment(TopicPartition topicPartition, String instance) {
        replicaPartitionToInstanceAssignment.put(topicPartition.partition(), instance);
        replicaPartitionsToAssign.remove(topicPartition.partition());
    }

    public void addMasterAssignment(TopicPartition topicPartition, String instance) {
        masterPartitionToInstanceAssignment.put(topicPartition.partition(), instance);
        masterPartitionsToAssign.remove(topicPartition.partition());
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
        masterPartitionsToAssign.remove(partition);
//        we don't update replicaPartitionsToAssign here as we don't want to assign them straight away, but rather using incremental rebalance
    }

    public int getNumberOfMasterPartitions() {
        return masterPartitionToInstanceAssignment.size() + masterPartitionsToAssign.size();
    }

    public int getNumberOfReplicaPartitions() {
        return replicaPartitionToInstanceAssignment.size() + replicaPartitionsToAssign.size();
    }

    public void removeMasterPartition(TopicPartition masterPartition) {
        int partition = masterPartition.partition();
        masterPartitionToInstanceAssignment.remove(partition);
        masterPartitionsToAssign.add(partition);
    }


    public void removeReplicaPartition(TopicPartition replicaPartition) {
        int partition = replicaPartition.partition();
        replicaPartitionToInstanceAssignment.remove(partition);
        replicaPartitionsToAssign.add(partition);
    }
}
