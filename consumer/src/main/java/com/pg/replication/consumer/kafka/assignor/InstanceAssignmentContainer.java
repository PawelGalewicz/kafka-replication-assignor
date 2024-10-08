package com.pg.replication.consumer.kafka.assignor;

import com.pg.replication.consumer.lifecycle.ApplicationStateContext;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.function.Consumer;

public class InstanceAssignmentContainer {

    private final Integer maxAssignmentsPerInstance;
    private final Integer masterTopicPartitionCount;
    private final Integer replicaTopicPartitionCount;

    Map<String, BitSet> consumerToPartitionAssignment = new HashMap<>();
    Map<String, AssignmentCount> instanceAssignmentCounter = new HashMap<>();
    Map<String, InstanceData> instanceToData = new HashMap<>();

    public InstanceAssignmentContainer(Integer maxAssignmentsPerInstance, Integer masterTopicPartitionCount, Integer replicaTopicPartitionCount) {
        this.masterTopicPartitionCount = masterTopicPartitionCount;
        this.replicaTopicPartitionCount = replicaTopicPartitionCount;
        this.maxAssignmentsPerInstance = maxAssignmentsPerInstance;
    }

    public BitSet getMasterPartitionSet(String instance) {
        String masterConsumer = instanceToData.get(instance).masterConsumer;
        return consumerToPartitionAssignment.computeIfAbsent(masterConsumer, ignore -> new BitSet(masterTopicPartitionCount));
    }

    public BitSet getReplicaPartitionSet(String instance) {
        String replicaConsumer = instanceToData.get(instance).replicaConsumer;
        return consumerToPartitionAssignment.computeIfAbsent(replicaConsumer, ignore -> new BitSet(replicaTopicPartitionCount));
    }

    public AssignmentCount getCount(String instance) {
        return instanceAssignmentCounter.get(instance);
    }

    public void addInstanceMasterConsumer(String instance,
                                          ApplicationStateContext.ApplicationState instanceState,
                                          String consumer) {
        if (!instanceToData.containsKey(instance)) {
            initialiseInstance(instance, instanceState);
        }
        InstanceData instanceData = instanceToData.get(instance);
        if (instanceData.masterConsumer == null) {
            instanceData.masterConsumer = consumer;
        }
    }

    public void addInstanceReplicaConsumer(String instance,
                                           ApplicationStateContext.ApplicationState instanceState,
                                           String consumer) {
        if (!instanceToData.containsKey(instance)) {
            initialiseInstance(instance, instanceState);
        }
        InstanceData instanceData = instanceToData.get(instance);
        if (instanceData.replicaConsumer == null) {
            instanceData.replicaConsumer = consumer;
        }
    }

    public void initialiseInstance(String instance, ApplicationStateContext.ApplicationState instanceState) {
        AssignmentCount initialCount = AssignmentCount.first(instance, maxAssignmentsPerInstance);
        instanceAssignmentCounter.put(instance, initialCount);
        instanceToData.put(instance, new InstanceData(instance, instanceState));
    }

    public void addReplicaAssignment(String instance, Integer replicaPartition) {
        BitSet instanceReplicaSet = getReplicaPartitionSet(instance);
        if (!instanceReplicaSet.get(replicaPartition)) {
            instanceReplicaSet.set(replicaPartition);
            modifyCounter(instance, AssignmentCount::incrementReplica);
        }
    }

    public void addMasterAssignment(String instance, Integer masterPartition) {
        BitSet instanceMasterSet = getMasterPartitionSet(instance);
        if (!instanceMasterSet.get(masterPartition)) {
            instanceMasterSet.set(masterPartition);
            modifyCounter(instance, AssignmentCount::incrementMaster);
        }
    }

    private void modifyCounter(String instance, Consumer<AssignmentCount> incrementFunction) {
        AssignmentCount instanceAssignmentCount = instanceAssignmentCounter.computeIfAbsent(instance, ignore -> AssignmentCount.first(instance, maxAssignmentsPerInstance));
        incrementFunction.accept(instanceAssignmentCount);
    }

    public void promoteReplicaToMaster(String instance, Integer partition) {
        getReplicaPartitionSet(instance).clear(partition);
        getMasterPartitionSet(instance).set(partition);
        modifyCounter(instance, AssignmentCount::incrementMasterDecrementReplica);
    }

    public Map<String, ConsumerPartitionAssignor.Assignment> buildAssignmentsByConsumer(String masterTopic, String replicaTopic) {
        Map<String, ConsumerPartitionAssignor.Assignment> partitionToConsumer = new HashMap<>(consumerToPartitionAssignment.size());

        instanceToData.values()
                .forEach(instanceData -> {
                    if (instanceData.masterConsumer != null) {
                        BitSet masterPartitions = consumerToPartitionAssignment.getOrDefault(instanceData.masterConsumer, new BitSet(0));
                        List<TopicPartition> topicPartitions = getTopicPartitions(masterPartitions, masterTopic);
                        partitionToConsumer.put(instanceData.masterConsumer, new ConsumerPartitionAssignor.Assignment(topicPartitions));
                    }

                    if (instanceData.replicaConsumer != null) {
                        BitSet replicaPartitions = consumerToPartitionAssignment.getOrDefault(instanceData.replicaConsumer, new BitSet(0));
                        List<TopicPartition> topicPartitions = getTopicPartitions(replicaPartitions, replicaTopic);
                        partitionToConsumer.put(instanceData.replicaConsumer, new ConsumerPartitionAssignor.Assignment(topicPartitions));
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
        return instanceToData.size();
    }

    public void removeMasterPartition(String instance, Integer masterPartition) {
        BitSet instanceMasterSet = getMasterPartitionSet(instance);
        if (instanceMasterSet.get(masterPartition)) {
            instanceMasterSet.clear(masterPartition);
            modifyCounter(instance, AssignmentCount::decrementMaster);
        }
    }

    public void removeReplicaPartition(String instance, Integer replicaPartition) {
        BitSet instanceReplicaSet = getReplicaPartitionSet(instance);
        if (instanceReplicaSet.get(replicaPartition)) {
            instanceReplicaSet.clear(replicaPartition);
            modifyCounter(instance, AssignmentCount::decrementReplica);
        }
    }

    public Collection<AssignmentCount> getInstanceAssignmentCounterValues() {
        return instanceAssignmentCounter.values();
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

    public static class InstanceData {
        String instanceName;
        String masterConsumer;
        String replicaConsumer;
        ApplicationStateContext.ApplicationState instanceState;

        public InstanceData(String instanceName, ApplicationStateContext.ApplicationState instanceState) {
            this.instanceName = instanceName;
            this.instanceState = instanceState;
        }
    }

}
