package com.pg.replication.consumer.v2;

import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.stream.Stream;

public class PartitionAssignmentContainer {

    private final TopicPartition topicPartition;

    private final int replicationFactor;
    private String masterConsumer;
    private final Set<String> replicaConsumers;

    public PartitionAssignmentContainer(String topic, int partition, int replicationFactor) {
        if (replicationFactor < 0) {
            throw new IllegalArgumentException("replication factor can't be negative");
        }

        this.topicPartition = new TopicPartition(topic, partition);
        this.replicationFactor = replicationFactor;
        this.replicaConsumers = new HashSet<>(replicationFactor);
    }

    public boolean hasMaster() {
        return Objects.nonNull(masterConsumer);
    }

    public boolean hasAllReplicas() {
        return replicaConsumers.size() == replicationFactor;
    }

    public int countMissingReplicas() {
        return replicationFactor - replicaConsumers.size();
    }

    public boolean addReplica(String consumer) {
        if (hasAllReplicas() || consumer.equals(masterConsumer)) {
            return false;
        }

        return replicaConsumers.add(consumer);
    }

    public boolean addMaster(String consumer) {
        if (hasMaster() || replicaConsumers.contains(consumer)) {
            return false;
        }

        masterConsumer = consumer;
        return true;
    }

    public boolean promoteReplicaToMaster(String consumer) {
//        todo do zrobienia
        return false;
    }

    public boolean demoteMasterToReplica(String consumer) {
//        todo do zrobienia
        return false;
    }

    public Integer getPartition() {
        return topicPartition.partition();
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public Optional<String> getMaster() {
        return Optional.ofNullable(masterConsumer);
    }

    public Set<String> getReplicas() {
        return replicaConsumers;
    }

    public Stream<Map.Entry<String, PartitionAssignment>> getConsumerPartitionAssignmentsStream() {
        Stream<Map.Entry<String, PartitionAssignment>> replicaAssignmentsStream = replicaConsumers.stream()
                .map(c -> Map.entry(c, new PartitionAssignment(topicPartition, ReplicationType.REPLICA)));

        Stream<Map.Entry<String, PartitionAssignment>> masterAssignmentsStream = Stream.of(masterConsumer)
                .map(c -> Map.entry(c, new PartitionAssignment(topicPartition, ReplicationType.MASTER)));


        return Stream.concat(replicaAssignmentsStream, masterAssignmentsStream);
    }
}
