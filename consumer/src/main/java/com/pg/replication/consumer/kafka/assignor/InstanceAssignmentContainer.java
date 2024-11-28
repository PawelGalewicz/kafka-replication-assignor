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

import static com.pg.replication.consumer.lifecycle.ApplicationStateContext.ApplicationState.*;

public class InstanceAssignmentContainer {

    private final Integer maxAssignmentsPerInstance;
    private final Integer masterTopicPartitionCount;
    private final Integer replicaTopicPartitionCount;

    private boolean terminatingInstanceExists = false;
    private boolean newInstanceExists = false;

    Map<String, BitSet> consumerToPartitionAssignment = new HashMap<>();
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
        return instanceToData.get(instance).assignmentCount;
    }

    public void addInstanceMasterConsumer(String instance,
                                          ApplicationStateContext.ApplicationDetails instanceDetails,
                                          String consumer) {
        if (!instanceToData.containsKey(instance)) {
            initialiseInstance(instance, instanceDetails);
        }
        InstanceData instanceData = instanceToData.get(instance);
        if (instanceData.masterConsumer == null) {
            instanceData.masterConsumer = consumer;
        }
    }

    public void addInstanceReplicaConsumer(String instance,
                                           ApplicationStateContext.ApplicationDetails instanceDetails,
                                           String consumer) {
        if (!instanceToData.containsKey(instance)) {
            initialiseInstance(instance, instanceDetails);
        }
        InstanceData instanceData = instanceToData.get(instance);
        if (instanceData.replicaConsumer == null) {
            instanceData.replicaConsumer = consumer;
        }
    }

    public void initialiseInstance(String instance, ApplicationStateContext.ApplicationDetails instanceDetails) {
        AssignmentCount initialCount = AssignmentCount.first(instance, maxAssignmentsPerInstance);
        instanceToData.put(instance, new InstanceData(instance, instanceDetails, initialCount));

        if (!terminatingInstanceExists && TERMINATING.equals(instanceDetails.getState())) {
            terminatingInstanceExists = true;
        } else if (!newInstanceExists && instanceDetails.isNew()) {
            newInstanceExists = true;
        }

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
        AssignmentCount instanceAssignmentCount = instanceToData.get(instance).assignmentCount;
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

    public Collection<InstanceData> getInstanceDataValues() {
        return instanceToData.values();
    }

    public InstanceData getInstanceData(String instance) {
        return instanceToData.get(instance);
    }

    public boolean terminatingInstanceExists() {
        return terminatingInstanceExists;
    }

    public boolean newInstanceExists() {
        return newInstanceExists;
    }


    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode
    public static class AssignmentCount {
        protected String instance;
        protected Integer masterCounter;
        protected Integer replicaCounter;
        protected Integer assignmentCounter;
        protected Integer maxCounter;

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
    }

    @Getter
    public static class InstanceData {
        private String instanceId;
        private String masterConsumer;
        private String replicaConsumer;
        private ApplicationStateContext.ApplicationDetails instanceDetails;
        private AssignmentCount assignmentCount;

        public InstanceData(String instanceId, ApplicationStateContext.ApplicationDetails instanceDetails, AssignmentCount initialCount) {
            this.instanceId = instanceId;
            this.instanceDetails = instanceDetails;
            this.assignmentCount = initialCount;
        }

        boolean isTerminating() {
            return TERMINATING.equals(instanceDetails.getState());
        }

        boolean isNotTerminating() {
            return !isTerminating();
        }

        public boolean isStable() {
            return STABLE.equals(instanceDetails.getState());
        }

        public boolean isNew() {
            return instanceDetails.isNew();
        }

        public static Comparator<InstanceData> masterThenAssignmentCountComparator() {
            return Comparator.comparingInt((InstanceData data) -> data.assignmentCount.getMasterCounter())
                    .thenComparingInt((InstanceData data) -> data.assignmentCount.getAssignmentCounter())
                    .thenComparing((InstanceData data) -> data.assignmentCount.getInstance());
        }

        public static Comparator<InstanceData> assignmentThenMasterCountComparator() {
            return Comparator.comparingInt((InstanceData data) -> data.assignmentCount.getAssignmentCounter())
                    .thenComparingInt((InstanceData data) -> data.assignmentCount.getMasterCounter())
                    .thenComparing((InstanceData data) -> data.assignmentCount.getInstance());
        }
    }

}
