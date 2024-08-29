package com.pg.replication.consumer.kafka.assignment.v1;

import lombok.*;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static java.util.Collections.max;

public class InstanceAssignmentContainer {

    private final Integer maxAssignmentsPerInstance;

//    fixme bitsets can be used for the 2 first maps here
    Map<String, Set<TopicPartition>> instanceToMasterPartitionAssignment = new HashMap<>();
    Map<String, Set<TopicPartition>> instanceToReplicaPartitionAssignment = new HashMap<>();
    @Getter
    Map<String, InstanceAssignmentCount> instanceAssignmentCounter = new HashMap<>();
    Map<String, InstanceConsumers> instanceToConsumers = new HashMap<>();

    public InstanceAssignmentContainer(Integer maxAssignmentsPerInstance) {
        this.maxAssignmentsPerInstance = maxAssignmentsPerInstance;
    }

    public Set<TopicPartition> getMasterPartitions(String instance) {
        return instanceToMasterPartitionAssignment.get(instance);
    }

    public Set<TopicPartition> getReplicaPartitions(String instance) {
        return instanceToReplicaPartitionAssignment.get(instance);
    }

    public InstanceAssignmentCount getCount(String instance) {
        return instanceAssignmentCounter.get(instance);
    }

    public void addInstanceMasterConsumer(String instance, String consumer) {
        if (!instanceToConsumers.containsKey(instance)) {
            initialiseInstance(instance);
        }
        InstanceConsumers instanceConsumers = instanceToConsumers.get(instance);
        if (instanceConsumers != null) {
            instanceConsumers.masterConsumer = consumer;
        }
    }

    public void addInstanceReplicaConsumer(String instance, String consumer) {
        if (!instanceToConsumers.containsKey(instance)) {
            initialiseInstance(instance);
        }
        InstanceConsumers instanceConsumers = instanceToConsumers.get(instance);
        if (instanceConsumers != null) {
            instanceConsumers.replicaConsumer = consumer;
        }
    }

    public void initialiseInstance(String instance) {
        InstanceAssignmentCount initialCount = InstanceAssignmentCount.first(instance, maxAssignmentsPerInstance);
        instanceAssignmentCounter.put(instance, initialCount);
        instanceToConsumers.put(instance, new InstanceConsumers());
    }

    public boolean canAddAssignment(String instance) {
        return instanceAssignmentCounter.getOrDefault(instance, InstanceAssignmentCount.first(instance, maxAssignmentsPerInstance)).canIncrement();
    }

    public void addReplicaAssignment(TopicPartition topicPartition, String instance) {
        if (instanceToReplicaPartitionAssignment.computeIfAbsent(instance, ignore -> new HashSet<>()).add(topicPartition)) {
            modifyCounter(instance, InstanceAssignmentCount::incrementReplica);
        }
    }

    public void addMasterAssignment(TopicPartition topicPartition, String instance) {
        if (instanceToMasterPartitionAssignment.computeIfAbsent(instance, ignore -> new HashSet<>()).add(topicPartition)) {
            modifyCounter(instance, InstanceAssignmentCount::incrementMaster);
        }
    }

    private void modifyCounter(String instance, Consumer<InstanceAssignmentCount> incrementFunction) {
        InstanceAssignmentCount instanceAssignmentCount = instanceAssignmentCounter.computeIfAbsent(instance, ignore -> InstanceAssignmentCount.first(instance, maxAssignmentsPerInstance));
        incrementFunction.accept(instanceAssignmentCount);
    }

    public void promoteReplicaToMaster(String instance, TopicPartition replicaPartition, TopicPartition masterPartition) {
        instanceToReplicaPartitionAssignment.computeIfAbsent(instance, ignore -> new HashSet<>()).remove(replicaPartition);
        instanceToMasterPartitionAssignment.computeIfAbsent(instance, ignore -> new HashSet<>()).add(masterPartition);
        modifyCounter(instance, InstanceAssignmentCount::incrementMasterDecrementReplica);
    }

    public Map<String, ConsumerPartitionAssignor.Assignment> getAssignmentsByConsumer() {
        Stream<Map.Entry<String, Set<TopicPartition>>> masterConsumerPartitionAssignments = instanceToConsumers.entrySet()
                .stream()
                .filter(e -> e.getValue().masterConsumer != null)
                .map(e -> Map.entry(e.getValue().masterConsumer, instanceToMasterPartitionAssignment.getOrDefault(e.getKey(), emptySet())));

        Stream<Map.Entry<String, Set<TopicPartition>>> replicaConsumerPartitionAssignments = instanceToConsumers.entrySet()
                .stream()
                .filter(e -> e.getValue().replicaConsumer != null)
                .map(e -> Map.entry(e.getValue().replicaConsumer, instanceToReplicaPartitionAssignment.getOrDefault(e.getKey(), emptySet())));

        return Stream.concat(masterConsumerPartitionAssignments, replicaConsumerPartitionAssignments)
                .collect(
                        Collectors.groupingBy(Map.Entry::getKey,
                                Collectors.flatMapping(e -> e.getValue().stream(),
                                        Collectors.collectingAndThen(Collectors.toList(),
                                                ConsumerPartitionAssignor.Assignment::new))
                        )
                );
    }

    public Set<String> getInstances() {
        return instanceAssignmentCounter.keySet();
    }

    public int getNumberOfInstances() {
        return instanceToConsumers.size();
    }

    public void removeMasterPartition(String instance, TopicPartition topicPartition) {
        if (instanceToMasterPartitionAssignment.get(instance).remove(topicPartition)) {
            modifyCounter(instance, InstanceAssignmentCount::decrementMaster);
        }
    }

    public void removeReplicaPartition(String instance, TopicPartition topicPartition) {
        if (instanceToReplicaPartitionAssignment.get(instance).remove(topicPartition)) {
            modifyCounter(instance, InstanceAssignmentCount::decrementReplica);
        }
    }

    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder(toBuilder = true)
    @EqualsAndHashCode
    public static class InstanceAssignmentCount {
        String instance;
        Integer masterCounter;
        Integer replicaCounter;
        Integer assignmentCounter;
        Integer maxCounter;

        public InstanceAssignmentCount(String instance, Integer maxCounter) {
            this.instance = instance;
            this.masterCounter = 0;
            this.replicaCounter = 0;
            this.assignmentCounter = 0;
            this.maxCounter = maxCounter;
        }

        public void incrementMaster() {
            masterCounter += 1;
            assignmentCounter += 1;
        }

        public void incrementReplica() {
            replicaCounter += 1;
            assignmentCounter += 1;
        }

        public void decrementMaster() {
            masterCounter -= 1;
            assignmentCounter -= 1;
        }

        public void decrementReplica() {
            replicaCounter -= 1;
            assignmentCounter -= 1;
        }

        public void incrementMasterDecrementReplica() {
            masterCounter += 1;
            replicaCounter -= 1;
        }

        public boolean canIncrement() {
            return assignmentCounter < maxCounter;
        }

        public static InstanceAssignmentCount first(String instance, Integer maxCounter) {
            return new InstanceAssignmentCount(instance, maxCounter);
        }

        public static Comparator<InstanceAssignmentCount> masterThenAssignmentCountComparator() {
            return Comparator.comparingInt(InstanceAssignmentCount::getMasterCounter)
                    .thenComparingInt(InstanceAssignmentCount::getAssignmentCounter)
                    .thenComparing(InstanceAssignmentCount::getInstance);
        }

        public static Comparator<InstanceAssignmentCount> assignmentThenMasterCountComparator() {
            return Comparator.comparingInt(InstanceAssignmentCount::getAssignmentCounter)
                    .thenComparingInt(InstanceAssignmentCount::getMasterCounter)
                    .thenComparing(InstanceAssignmentCount::getInstance);
        }
    }

    public static class InstanceConsumers {
        String masterConsumer;
        String replicaConsumer;
    }

    public static class SortedLinkedList<T> extends LinkedList<T> {

        private final Comparator<T> comparator;

        public SortedLinkedList(Comparator<T> comparator) {
            super();
            this.comparator = comparator;
        }

        @Override
        public boolean add(T t) {
            int computedIndex = Collections.binarySearch(this, t, comparator);
            if (computedIndex < 0) {
                computedIndex = -(computedIndex + 1);
            }

            if (computedIndex > this.size()) {
                addLast(t);
            }

            add(computedIndex, t);
            return true;
        }
    }
}
