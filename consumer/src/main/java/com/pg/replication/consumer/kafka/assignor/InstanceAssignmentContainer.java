package com.pg.replication.consumer.kafka.assignor;

import lombok.*;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InstanceAssignmentContainer {
    private static final BitSet EMPTY_BITSET = new BitSet(0);

    private final Integer maxAssignmentsPerInstance;
    private final Integer masterTopicPartitionCount;
    private final Integer replicaTopicPartitionCount;

    Map<String, BitSet> instanceToMasterPartitionAssignment = new HashMap<>();
    Map<String, BitSet> instanceToReplicaPartitionAssignment = new HashMap<>();
    @Getter
    Map<String, InstanceAssignmentCount> instanceAssignmentCounter = new HashMap<>();
    Map<String, InstanceConsumers> instanceToConsumers = new HashMap<>();

    public InstanceAssignmentContainer(Integer maxAssignmentsPerInstance, Integer masterTopicPartitionCount, Integer replicaTopicPartitionCount) {
        this.masterTopicPartitionCount = masterTopicPartitionCount;
        this.replicaTopicPartitionCount = replicaTopicPartitionCount;
        this.maxAssignmentsPerInstance = maxAssignmentsPerInstance;
    }

    public BitSet getMasterPartitionSet(String instance) {
        return instanceToMasterPartitionAssignment.getOrDefault(instance, EMPTY_BITSET);
    }

    public BitSet getReplicaPartitionSet(String instance) {
        return instanceToReplicaPartitionAssignment.getOrDefault(instance, EMPTY_BITSET);
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

    public void addReplicaAssignment(String instance, Integer replicaPartition) {
        BitSet instanceReplicaSet = instanceToReplicaPartitionAssignment.computeIfAbsent(instance, ignore -> new BitSet(replicaTopicPartitionCount));
        if (!instanceReplicaSet.get(replicaPartition)) {
            instanceReplicaSet.set(replicaPartition);
            modifyCounter(instance, InstanceAssignmentCount::incrementReplica);
        }
    }

    public void addMasterAssignment(String instance, Integer masterPartition) {
        BitSet instanceMasterSet = instanceToMasterPartitionAssignment.computeIfAbsent(instance, ignore -> new BitSet(masterTopicPartitionCount));
        if (!instanceMasterSet.get(masterPartition)) {
            instanceMasterSet.set(masterPartition);
            modifyCounter(instance, InstanceAssignmentCount::incrementMaster);
        }
    }

    private void modifyCounter(String instance, Consumer<InstanceAssignmentCount> incrementFunction) {
        InstanceAssignmentCount instanceAssignmentCount = instanceAssignmentCounter.computeIfAbsent(instance, ignore -> InstanceAssignmentCount.first(instance, maxAssignmentsPerInstance));
        incrementFunction.accept(instanceAssignmentCount);
    }

    public void promoteReplicaToMaster(String instance, Integer partition) {
        instanceToReplicaPartitionAssignment.computeIfAbsent(instance, ignore -> new BitSet(replicaTopicPartitionCount)).clear(partition);
        instanceToMasterPartitionAssignment.computeIfAbsent(instance, ignore -> new BitSet(masterTopicPartitionCount)).set(partition);
        modifyCounter(instance, InstanceAssignmentCount::incrementMasterDecrementReplica);
    }

    public Map<String, ConsumerPartitionAssignor.Assignment> getAssignmentsByConsumer(String masterTopic, String replicaTopic) {
        Stream<Map.Entry<String, Stream<TopicPartition>>> masterConsumerPartitionAssignments = instanceToConsumers.entrySet()
                .stream()
                .filter(e -> e.getValue().masterConsumer != null)
                .map(e -> {
                    BitSet masterPartitionSet = instanceToMasterPartitionAssignment.getOrDefault(e.getKey(), EMPTY_BITSET);
                    return Map.entry(e.getValue().masterConsumer, getTopicPartitions(masterPartitionSet, masterTopic));
                });

        Stream<Map.Entry<String, Stream<TopicPartition>>> replicaConsumerPartitionAssignments = instanceToConsumers.entrySet()
                .stream()
                .filter(e -> e.getValue().replicaConsumer != null)
                .map(e -> {
                    BitSet replicaPartitionSet = instanceToReplicaPartitionAssignment.getOrDefault(e.getKey(), EMPTY_BITSET);
                    return Map.entry(e.getValue().replicaConsumer, getTopicPartitions(replicaPartitionSet, replicaTopic));
                });
        return Stream.concat(masterConsumerPartitionAssignments, replicaConsumerPartitionAssignments)
                .collect(
                        Collectors.groupingBy(Map.Entry::getKey,
                                Collectors.flatMapping(Map.Entry::getValue,
                                        Collectors.collectingAndThen(Collectors.toList(),
                                                ConsumerPartitionAssignor.Assignment::new))
                        )
                );
    }

    private Stream<TopicPartition> getTopicPartitions(BitSet set, String topicName) {
        return set.stream()
                .mapToObj(partition -> new TopicPartition(topicName, partition));
    }

    public int getNumberOfInstances() {
        return instanceToConsumers.size();
    }

    public void removeMasterPartition(String instance, Integer masterPartition) {
        BitSet instanceMasterSet = instanceToMasterPartitionAssignment.get(instance);
        if (instanceMasterSet.get(masterPartition)) {
            instanceMasterSet.clear(masterPartition);
            modifyCounter(instance, InstanceAssignmentCount::decrementMaster);
        }
    }

    public void removeReplicaPartition(String instance, Integer replicaPartition) {
        BitSet instanceReplicaSet = instanceToReplicaPartitionAssignment.get(instance);
        if (instanceReplicaSet.get(replicaPartition)) {
            instanceReplicaSet.clear(replicaPartition);
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
