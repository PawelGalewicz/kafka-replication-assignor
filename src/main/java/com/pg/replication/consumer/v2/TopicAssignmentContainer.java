package com.pg.replication.consumer.v2;

import org.apache.commons.lang3.stream.IntStreams;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TopicAssignmentContainer {
    final String topic;
    final int numberOfPartitions;
    final int partitionReplicationFactor;
    Map<Integer, PartitionAssignmentContainer> partitionToAssignmentContainer;
    Map<String, ConsumerAssignmentCounter> consumerAssignmentCounter = new HashMap<>();

    public TopicAssignmentContainer(String topic, int numberOfPartitions, int partitionReplicationFactor) {
        this.topic = topic;
        this.numberOfPartitions = numberOfPartitions;
        this.partitionReplicationFactor = partitionReplicationFactor;
        this.partitionToAssignmentContainer = buildTopicPartitionContainer();
    }

    private Map<Integer, PartitionAssignmentContainer> buildTopicPartitionContainer() {
        return IntStreams.range(numberOfPartitions)
                .mapToObj(p -> new PartitionAssignmentContainer(topic, p, partitionReplicationFactor))
                .collect(Collectors.toMap(PartitionAssignmentContainer::getPartition, Function.identity()));
    }

    public Collection<PartitionAssignmentContainer> getPartitionAssignments() {
        return partitionToAssignmentContainer.values();
    }

    public boolean addPartitionMaster(Integer partition, String consumer) {
        validatePartitionNumber(partition);

        if (partitionToAssignmentContainer.get(partition).addMaster(consumer)) {
            consumerAssignmentCounter.computeIfAbsent(consumer, ConsumerAssignmentCounter::new).incrementMasters();
            return true;
        }

        return false;
    }

    public boolean addPartitionReplica(Integer partition, String consumer) {
        validatePartitionNumber(partition);

        if (partitionToAssignmentContainer.get(partition).addReplica(consumer)) {
            consumerAssignmentCounter.computeIfAbsent(consumer, ConsumerAssignmentCounter::new).incrementReplicas();
            return true;
        }

        return false;
    }

    public boolean promoteReplicaToMaster(Integer partition, String consumer) {
        validatePartitionNumber(partition);

        if (partitionToAssignmentContainer.get(partition).promoteReplicaToMaster(consumer)) {
            ConsumerAssignmentCounter consumerAssignmentCounter = this.consumerAssignmentCounter.computeIfAbsent(consumer, ConsumerAssignmentCounter::new);
            consumerAssignmentCounter.incrementMasters();
            consumerAssignmentCounter.decrementReplicas();
            return true;
        }

        return false;
    }

    public boolean demoteMasterToReplica(Integer partition, String consumer) {
        validatePartitionNumber(partition);

        if (partitionToAssignmentContainer.get(partition).demoteMasterToReplica(consumer)) {
            ConsumerAssignmentCounter consumerAssignmentCounter = this.consumerAssignmentCounter.computeIfAbsent(consumer, ConsumerAssignmentCounter::new);
            consumerAssignmentCounter.decrementMasters();
            consumerAssignmentCounter.incrementReplicas();
            return true;
        }

        return false;
    }

    private void validatePartitionNumber(Integer partition) {
        if (partition < 0 || partition >= numberOfPartitions) {
            throw new IllegalArgumentException("incorrect partition");
        }
    }

    public Collection<String> getReplicaConsumers(Integer partition) {
        return partitionToAssignmentContainer.get(partition).getReplicas();
    }

    public Optional<String> getMasterConsumer(Integer partition) {
        return partitionToAssignmentContainer.get(partition).getMaster();
    }

    public Integer getMasterAssignments(String consumer) {
//        return consumerMasters.getOrDefault(consumer, 0);
        return consumerAssignmentCounter.get(consumer).getNumberOfMasters();
    }

    public Integer getReplicaAssignments(String consumer) {
//        return consumerReplicas.getOrDefault(consumer, 0);
        return consumerAssignmentCounter.get(consumer).getNumberOfReplicas();
    }

    public Integer getPartitionAssignments(String consumer) {
        return consumerAssignmentCounter.get(consumer).getNumberOfPartitions();
    }

    public Stream<Map.Entry<String, PartitionAssignment>> getConsumerPartitionAssignmentsStream() {
        return partitionToAssignmentContainer.values().stream()
                .flatMap(PartitionAssignmentContainer::getConsumerPartitionAssignmentsStream);
    }
}
