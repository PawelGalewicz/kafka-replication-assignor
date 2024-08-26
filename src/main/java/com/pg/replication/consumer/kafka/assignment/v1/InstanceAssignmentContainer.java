package com.pg.replication.consumer.kafka.assignment.v1;

import lombok.*;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static java.util.Collections.max;

public class InstanceAssignmentContainer {

    private final Integer maxAssignmentsPerInstance;

    Map<String, Set<TopicPartition>> instanceToMasterPartitionAssignment = new HashMap<>();
    Map<String, Set<TopicPartition>> instanceToReplicaPartitionAssignment = new HashMap<>();
    @Getter
    SortedLinkedList<InstanceAssignmentCount> instanceAssignmentCount = new SortedLinkedList<>((one, two) -> one.compareTo(two, InstanceAssignmentCount::getAssignmentCounter));
    @Getter
    SortedLinkedList<InstanceAssignmentCount> masterAssignmentCount = new SortedLinkedList<>((one, two) -> one.compareTo(two, InstanceAssignmentCount::getMasterCounter));
    @Getter
    SortedLinkedList<InstanceAssignmentCount> replicaAssignmentCount = new SortedLinkedList<>((one, two) -> one.compareTo(two, InstanceAssignmentCount::getReplicaCounter));
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

    public void addInstanceMaster(String instance, String consumer) {
        if (!instanceToConsumers.containsKey(instance)) {
            initialiseInstance(instance);
        }
        InstanceConsumers instanceConsumers = instanceToConsumers.get(instance);
        if (instanceConsumers != null) {
            instanceConsumers.masterConsumer = consumer;
        }
    }

    public void addInstanceReplica(String instance, String consumer) {
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
        instanceAssignmentCount.add(initialCount);
        masterAssignmentCount.add(initialCount);
        replicaAssignmentCount.add(initialCount);
        instanceAssignmentCounter.put(instance, initialCount);
        instanceToConsumers.put(instance, new InstanceConsumers());
    }

//    public void addOtherAssignment(TopicPartition topicPartition, String instance, String consumer) {
//        consumerToOtherPartitionAssignment.computeIfAbsent(consumer, ignore -> new HashSet<>()).add(topicPartition);
//        incrementCounter(instance);
//    }

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

    private void modifyCounter(String instance, Function<InstanceAssignmentCount, InstanceAssignmentCount> incrementFunction) {
        InstanceAssignmentCount count = instanceAssignmentCounter.getOrDefault(instance, InstanceAssignmentCount.first(instance, maxAssignmentsPerInstance));
        InstanceAssignmentCount newCount = incrementFunction.apply(count);
        modifySortedCounters(count, newCount);
        instanceAssignmentCounter.put(instance, newCount);
    }

    private void modifySortedCounters(InstanceAssignmentCount count, InstanceAssignmentCount newCount) {
        instanceAssignmentCount.remove(count);
        instanceAssignmentCount.add(newCount);

        masterAssignmentCount.remove(count);
        masterAssignmentCount.add(newCount);

        replicaAssignmentCount.remove(count);
        replicaAssignmentCount.add(newCount);
    }

    public String getLeastAssigned() {
        return instanceAssignmentCount.getLast().getInstance();
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
//        fixme compare instance ids in comparison and maybe then treesets will work
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

//        fixme add static method to build comparators
        public int compareTo(InstanceAssignmentCount o, Function<InstanceAssignmentCount, Integer> comparingValueSupplier) {
            return comparingValueSupplier.apply(o).compareTo(comparingValueSupplier.apply(this));
        }

        public InstanceAssignmentCount incrementMaster() {
            return this.toBuilder()
                    .masterCounter(masterCounter + 1)
                    .assignmentCounter(assignmentCounter + 1)
                    .build();
        }

        public InstanceAssignmentCount incrementReplica() {
            return this.toBuilder()
                    .replicaCounter(replicaCounter + 1)
                    .assignmentCounter(assignmentCounter + 1)
                    .build();
        }

        public InstanceAssignmentCount decrementMaster() {
            return this.toBuilder()
                    .masterCounter(masterCounter - 1)
                    .assignmentCounter(assignmentCounter - 1)
                    .build();
        }

        public InstanceAssignmentCount decrementReplica() {
            return this.toBuilder()
                    .replicaCounter(replicaCounter - 1)
                    .assignmentCounter(assignmentCounter - 1)
                    .build();
        }

        public InstanceAssignmentCount incrementMasterDecrementReplica() {
            return this.toBuilder()
                    .masterCounter(masterCounter + 1)
                    .replicaCounter(replicaCounter - 1)
                    .build();
        }

        public static InstanceAssignmentCount first(String instance, Integer maxCounter) {
            return new InstanceAssignmentCount(instance, maxCounter);
        }

        public boolean canIncrement() {
            return assignmentCounter < maxCounter;
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
