package com.pg.replication.consumer.kafka.assignor;

import lombok.*;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InstanceAssignmentContainer {

    private final Integer maxAssignmentsPerInstance;
    private final Integer masterTopicPartitionCount;
    private final Integer replicaTopicPartitionCount;

    Map<String, BitSet> consumerToPartitionAssignment = new HashMap<>();
    Map<String, InstanceAssignmentCount> instanceAssignmentCounter = new HashMap<>();
    Map<String, InstanceConsumers> instanceToConsumers = new HashMap<>();
    Map<String, InstanceConsumers> instanceDuplicatesToConsumers = new HashMap<>();

    public InstanceAssignmentContainer(Integer maxAssignmentsPerInstance, Integer masterTopicPartitionCount, Integer replicaTopicPartitionCount) {
        this.masterTopicPartitionCount = masterTopicPartitionCount;
        this.replicaTopicPartitionCount = replicaTopicPartitionCount;
        this.maxAssignmentsPerInstance = maxAssignmentsPerInstance;
    }

    public BitSet getMasterPartitionSet(String instance) {
        String masterConsumer = instanceToConsumers.get(instance).masterConsumer;
        return consumerToPartitionAssignment.computeIfAbsent(masterConsumer, ignore -> new BitSet(masterTopicPartitionCount));
    }

    public BitSet getReplicaPartitionSet(String instance) {
        String replicaConsumer = instanceToConsumers.get(instance).replicaConsumer;
        return consumerToPartitionAssignment.computeIfAbsent(replicaConsumer, ignore -> new BitSet(replicaTopicPartitionCount));
    }

    public Set<String> getInstancesWithDuplicates() {
        return instanceDuplicatesToConsumers.keySet();
    }

    public AssignmentCount getCount(String instance) {
        return instanceAssignmentCounter.get(instance).getInstanceCount();
    }

    public AssignmentCount getDuplicateCount(String instance) {
        return instanceAssignmentCounter.get(instance).getDuplicateCount();
    }

    public void addInstanceMasterConsumer(String instance, String consumer) {
        if (!instanceToConsumers.containsKey(instance)) {
            initialiseInstance(instance);
        }
        InstanceConsumers instanceConsumers = instanceToConsumers.get(instance);
        if (instanceConsumers.masterConsumer == null) {
            instanceConsumers.masterConsumer = consumer;
        } else {
            addInstanceDuplicateMasterConsumer(instance, consumer);
        }
    }

    private void addInstanceDuplicateMasterConsumer(String instance, String consumer) {
        InstanceConsumers duplicateConsumers = instanceDuplicatesToConsumers.computeIfAbsent(instance, ignore -> new InstanceConsumers());
        duplicateConsumers.masterConsumer = consumer;
    }

    public void addInstanceReplicaConsumer(String instance, String consumer) {
        if (!instanceToConsumers.containsKey(instance)) {
            initialiseInstance(instance);
        }
        InstanceConsumers instanceConsumers = instanceToConsumers.get(instance);
        if (instanceConsumers.replicaConsumer == null) {
            instanceConsumers.replicaConsumer = consumer;
        } else {
            addInstanceDuplicateReplicaConsumer(instance, consumer);
        }
    }

    private void addInstanceDuplicateReplicaConsumer(String instance, String consumer) {
        InstanceConsumers duplicateConsumers = instanceDuplicatesToConsumers.computeIfAbsent(instance, ignore -> new InstanceConsumers());
        duplicateConsumers.replicaConsumer = consumer;
    }

    public void initialiseInstance(String instance) {
        InstanceAssignmentCount initialCount = InstanceAssignmentCount.first(instance, maxAssignmentsPerInstance);
        instanceAssignmentCounter.put(instance, initialCount);
        instanceToConsumers.put(instance, new InstanceConsumers());
    }

    public void addReplicaAssignment(String instance, Integer replicaPartition) {
        BitSet instanceReplicaSet = getReplicaPartitionSet(instance);
        if (!instanceReplicaSet.get(replicaPartition)) {
            instanceReplicaSet.set(replicaPartition);
            modifyCounter(instance, InstanceAssignmentCount::incrementReplica);
        }
    }

    public void addReplicaPartitionToDuplicate(String instance, Integer replicaPartition) {
        InstanceConsumers instanceDuplicateConsumers = instanceDuplicatesToConsumers.get(instance);
        if (instanceDuplicateConsumers != null && instanceDuplicateConsumers.replicaConsumer != null) {
            BitSet duplicateReplicaSet = consumerToPartitionAssignment.computeIfAbsent(instanceDuplicateConsumers.replicaConsumer, ignore -> new BitSet(replicaTopicPartitionCount));
            duplicateReplicaSet.set(replicaPartition);
            modifyCounter(instance, InstanceAssignmentCount::incrementDuplicateReplica);
        }
    }

    public void addMasterAssignment(String instance, Integer masterPartition) {
        BitSet instanceMasterSet = getMasterPartitionSet(instance);
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
        getReplicaPartitionSet(instance).clear(partition);
        getMasterPartitionSet(instance).set(partition);
        modifyCounter(instance, InstanceAssignmentCount::incrementMasterDecrementReplica);
    }

    public Map<String, ConsumerPartitionAssignor.Assignment> buildAssignmentsByConsumer(String masterTopic, String replicaTopic) {
        Map<String, ConsumerPartitionAssignor.Assignment> partitionToConsumer = new HashMap<>(consumerToPartitionAssignment.size());

        Stream.concat(instanceToConsumers.values().stream(), instanceDuplicatesToConsumers.values().stream())
                .forEach(instanceConsumers -> {
                    if (instanceConsumers.masterConsumer != null) {
                        BitSet masterPartitions = consumerToPartitionAssignment.getOrDefault(instanceConsumers.masterConsumer, new BitSet(0));
                        List<TopicPartition> topicPartitions = getTopicPartitions(masterPartitions, masterTopic);
                        partitionToConsumer.put(instanceConsumers.masterConsumer, new ConsumerPartitionAssignor.Assignment(topicPartitions));
                    }

                    if (instanceConsumers.replicaConsumer != null) {
                        BitSet replicaPartitions = consumerToPartitionAssignment.getOrDefault(instanceConsumers.replicaConsumer, new BitSet(0));
                        List<TopicPartition> topicPartitions = getTopicPartitions(replicaPartitions, replicaTopic);
                        partitionToConsumer.put(instanceConsumers.replicaConsumer, new ConsumerPartitionAssignor.Assignment(topicPartitions));
                    }
                });

        return partitionToConsumer;
    }

    private List<TopicPartition> getTopicPartitions(BitSet set, String topicName) {
        return set.stream()
                .mapToObj(partition -> new TopicPartition(topicName, partition))
                .toList();
    }

    public int getNumberOfInstances() {
        return instanceToConsumers.size();
    }

    public void removeMasterPartition(String instance, Integer masterPartition) {
        BitSet instanceMasterSet = getMasterPartitionSet(instance);
        if (instanceMasterSet.get(masterPartition)) {
            instanceMasterSet.clear(masterPartition);
            modifyCounter(instance, InstanceAssignmentCount::decrementMaster);
        }
    }

    public void removeReplicaPartition(String instance, Integer replicaPartition) {
        BitSet instanceReplicaSet = getReplicaPartitionSet(instance);
        if (instanceReplicaSet.get(replicaPartition)) {
            instanceReplicaSet.clear(replicaPartition);
            modifyCounter(instance, InstanceAssignmentCount::decrementReplica);
        }
    }

    public Set<AssignmentCount> getInstanceAssignmentCounterValues() {
        return instanceAssignmentCounter.values().stream().map(InstanceAssignmentCount::getInstanceCount).collect(Collectors.toSet());
    }

    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode
    public static class InstanceAssignmentCount {
        AssignmentCount instanceCount;
        AssignmentCount duplicateCount;

        public void incrementMaster() {
            instanceCount.incrementMaster();
        }

        public void incrementReplica() {
            instanceCount.incrementReplica();
        }

        public void decrementMaster() {
            instanceCount.decrementMaster();
        }

        public void decrementReplica() {
            instanceCount.decrementReplica();
        }

        public void incrementMasterDecrementReplica() {
            instanceCount.incrementMasterDecrementReplica();
        }

        public void incrementDuplicateReplica() {
            duplicateCount.incrementReplica();
        }

        public static InstanceAssignmentCount first(String instance, Integer maxAssignmentsPerInstance) {
            return new InstanceAssignmentCount(AssignmentCount.first(instance, maxAssignmentsPerInstance), AssignmentCount.first(instance, maxAssignmentsPerInstance));
        }
    }

    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode
    public static class AssignmentCount {
        String instance;
        Integer masterCounter;
        Integer replicaCounter;
        Integer assignmentCounter;
        Integer maxCounter;

        public AssignmentCount(String instance, Integer maxCounter) {
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

        public static AssignmentCount first(String instance, Integer maxCounter) {
            return new AssignmentCount(instance, maxCounter);
        }

        public static Comparator<AssignmentCount> masterThenAssignmentCountComparator() {
            return Comparator.comparingInt(AssignmentCount::getMasterCounter)
                    .thenComparingInt(AssignmentCount::getAssignmentCounter)
                    .thenComparing(AssignmentCount::getInstance);
        }

        public static Comparator<AssignmentCount> assignmentThenMasterCountComparator() {
            return Comparator.comparingInt(AssignmentCount::getAssignmentCounter)
                    .thenComparingInt(AssignmentCount::getMasterCounter)
                    .thenComparing(AssignmentCount::getInstance);
        }
    }

    public static class InstanceConsumers {
        String masterConsumer;
        String replicaConsumer;
    }

}
