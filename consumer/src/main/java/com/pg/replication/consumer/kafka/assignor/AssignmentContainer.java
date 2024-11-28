package com.pg.replication.consumer.kafka.assignor;

import com.pg.replication.consumer.kafka.assignor.InstanceAssignmentContainer.AssignmentCount;
import com.pg.replication.consumer.kafka.assignor.InstanceAssignmentContainer.InstanceData;
import com.pg.replication.consumer.lifecycle.ApplicationStateContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.entry;

@Slf4j
public class AssignmentContainer {
    private final String masterTopic;
    private final String replicaTopic;

    private final PartitionAssignmentContainer partitionAssignmentContainer;
    private final InstanceAssignmentContainer instanceAssignmentContainer;

    public AssignmentContainer(String masterTopic,
                               String replicaTopic,
                               Integer maxAssignmentsPerInstance,
                               Integer masterTopicPartitionCount,
                               Integer replicaTopicPartitionCount) {
        this.masterTopic = masterTopic;
        this.replicaTopic = replicaTopic;

        this.instanceAssignmentContainer = new InstanceAssignmentContainer(maxAssignmentsPerInstance, masterTopicPartitionCount, replicaTopicPartitionCount);
        this.partitionAssignmentContainer = new PartitionAssignmentContainer(masterTopicPartitionCount, replicaTopicPartitionCount);
    }

    public void addInstanceConsumer(String instance,
                                    ApplicationStateContext.ApplicationDetails instanceDetails,
                                    String consumer,
                                    Collection<String> topics) {
        if (topics.contains(masterTopic)) {
            instanceAssignmentContainer.addInstanceMasterConsumer(instance, instanceDetails, consumer);
        } else if (topics.contains(replicaTopic)) {
            instanceAssignmentContainer.addInstanceReplicaConsumer(instance, instanceDetails, consumer);
        }
    }

    public void addAssignment(TopicPartition topicPartition, String instance) {
        if (masterTopic.equals(topicPartition.topic())) {
            addMasterAssignment(instance, topicPartition.partition());
        } else if (replicaTopic.equals(topicPartition.topic())) {
            addReplicaAssignment(instance, topicPartition.partition());
        }
    }

    private void addMasterAssignment(String instance, Integer masterPartition) {
        partitionAssignmentContainer.addMasterAssignment(instance, masterPartition);
        instanceAssignmentContainer.addMasterAssignment(instance, masterPartition);
    }

    private void addReplicaAssignment(String instance, Integer replicaPartition) {
        partitionAssignmentContainer.addReplicaAssignment(instance, replicaPartition);
        instanceAssignmentContainer.addReplicaAssignment(instance, replicaPartition);
    }

    public Map<String, ConsumerPartitionAssignor.Assignment> assign() {
        if (partitionAssignmentContainer.hasPendingAssignments()) {
//            If there are partitions that need assignment, assign them and return. Optimisations can be done in the next rebalance
            log.debug("There are partitions that are not assigned, will try to assign them");
            assignPendingPartitions();
        } else {
//            If all partitions are already assigned, we can see if there are any optimisations necessary.
            log.debug("All partitions are assigned, will try to optimise current assignment");
            optimiseAssignment();
        }

        if (instanceAssignmentContainer.terminatingInstanceExists()) {
            revokeAssignmentsFromTerminatingInstance();
        }

        return instanceAssignmentContainer.buildAssignmentsByConsumer(masterTopic, replicaTopic);
    }
//
    private void assignPendingPartitions() {
        BitSet masterPartitionsToAssign = (BitSet) partitionAssignmentContainer.getMasterPartitionsToAssign().clone();
        BitSet replicaPartitionsToAssign = (BitSet) partitionAssignmentContainer.getReplicaPartitionsToAssign().clone();

        log.debug("Assigning master partitions");

        for (int masterPartition = masterPartitionsToAssign.nextSetBit(0);
             masterPartition >= 0;
             masterPartition = masterPartitionsToAssign.nextSetBit(masterPartition + 1)
        ) {
            Optional<InstanceData> masterCandidateFromReplica = getReadyReplicaInstance(masterPartition);

//            master should not be moved if its replica is not ready
            if (masterCandidateFromReplica.isPresent()) {
                promoteReplicaToMaster(masterCandidateFromReplica.get().getInstanceId(), masterPartition);
                masterPartitionsToAssign.clear(masterPartition);
            }
        }

        log.debug("The following master partitions were not assigned due to no replicas available: {}", masterPartitionsToAssign);

        PriorityQueue<InstanceData> sortedInstances = getNonTerminatingInstancesSortedFromLeastToMostMastersAndAssignmentsQueue();

        masterPartitionsToAssign.and(replicaPartitionsToAssign);
        BitSet replicaPartitionsForUnassignedMasters = masterPartitionsToAssign;

        log.debug("Assigning replica partitions for unassigned masters");
        while (!sortedInstances.isEmpty()) {
            int replicaPartition = replicaPartitionsForUnassignedMasters.nextSetBit(0);
            if (replicaPartition < 0) {
                break;
            }

            InstanceData instance = sortedInstances.poll();
            AssignmentCount instanceCount = instance.getAssignmentCount();
            String instanceName = instanceCount.getInstance();
            if (instanceCount.canIncrement()) {
    //            If space available, just assign a replica to the instance
                addReplicaAssignment(instanceName, replicaPartition);
                sortedInstances.add(instance);
                replicaPartitionsForUnassignedMasters.clear(replicaPartition);
                replicaPartitionsToAssign.clear(replicaPartition);
            } else if (instanceCount.getReplicaCounter() > 0) {
    //            If no more space on the instance, but there is another replica of an assigned master, replace it as a replica for an unassigned master has higher priority
                OptionalInt replicaPartitionToReplace = instanceAssignmentContainer.getReplicaPartitionSet(instanceName)
                        .stream()
                        .filter(partition -> !masterPartitionsToAssign.get(partition))
                        .findFirst();

                if (replicaPartitionToReplace.isPresent()) {
                    forceReplicaAssignment(instanceName, replicaPartitionToReplace.getAsInt(), replicaPartition);
                    replicaPartitionsForUnassignedMasters.clear(replicaPartition);
                    replicaPartitionsToAssign.clear(replicaPartition);
                }
            }

//            fixme check if the instanceCount reference has increased
            if (instanceCount.canIncrement()) {
                sortedInstances.add(instance);
            }
        }

        log.debug("Assigning the rest of replicas");
        InstanceData skippedInstance = null;
        while (!sortedInstances.isEmpty()) {
            int replicaPartition = replicaPartitionsToAssign.nextSetBit(0);
            if (replicaPartition < 0) {
                break;
            }

            InstanceData instance = sortedInstances.poll();
            AssignmentCount instanceCount = instance.getAssignmentCount();
            String instanceName = instanceCount.getInstance();

            if (!instanceCount.canIncrement()) {
                continue;
            }

    //        If we skipped an instance already, then it means we already found a master and don't need to check for it again
            if (skippedInstance == null) {
                Boolean isInstanceMasterOfThisReplicaPartition = partitionAssignmentContainer.getMasterInstanceForPartition(replicaPartition)
                        .map(master -> master.equals(instanceName))
                        .orElse(Boolean.FALSE);

    //        A replica cannot be assigned to the instance that already has a master of that same partition
                if (isInstanceMasterOfThisReplicaPartition) {
                    skippedInstance = instance;
                    continue;
                }
            }

            addReplicaAssignment(instanceName, replicaPartition);
            replicaPartitionsToAssign.clear(replicaPartition);


            if (instanceCount.canIncrement()) {
                sortedInstances.add(instance);
            }

            if (skippedInstance != null && skippedInstance.getAssignmentCount().canIncrement()) {
                sortedInstances.add(skippedInstance);
                skippedInstance = null;
            }
        }

        if (!masterPartitionsToAssign.isEmpty()) {
            log.warn("The following master partitions were not assigned anywhere: [{}]", masterPartitionsToAssign);
        }

        if (!replicaPartitionsToAssign.isEmpty()) {
            log.warn("The following replica partitions were not assigned anywhere: [{}]", replicaPartitionsToAssign);
        }
    }

    private Optional<InstanceData> getReadyReplicaInstance(int masterPartition) {
        return partitionAssignmentContainer.getReplicaInstanceForPartition(masterPartition)
                .map(instanceAssignmentContainer::getInstanceData)
                .filter(InstanceData::isStable);
    }

    private Optional<InstanceData> getReplicaInstance(int masterPartition) {
        return partitionAssignmentContainer.getReplicaInstanceForPartition(masterPartition)
                .map(instanceAssignmentContainer::getInstanceData);
    }

    private void revokeAssignmentsFromTerminatingInstance() {
        List<InstanceData> terminatingInstances = getTerminatingInstances().toList();

        boolean newInstanceExists = instanceAssignmentContainer.newInstanceExists();

        for (InstanceData instance : terminatingInstances) {
            if (newInstanceExists) {
//                if new instance exists and an instance is terminating it most likely means there is a deployment happening.
//                if so, replicas for all masters of terminating instance needs to be moved to the new instances to prepare them
//                for migrating masters there. All replicas also need to be moved
                instanceAssignmentContainer.getMasterPartitionSet(instance.getInstanceId())
                        .stream()
                        .forEach(masterPartition -> {
                            Optional<InstanceData> replicaInstanceOptional = getReplicaInstance(masterPartition);
                            if (replicaInstanceOptional.isEmpty()) {
//                                if there is no replica, we don't do anything
                                return;
                            }

                            InstanceData replicaInstance = replicaInstanceOptional.get();

                            if (!replicaInstance.isNew()) {
//                                if the replica is not in the new instance we want to move that replica to the new instance
                                revokeReplicaAssignment(replicaInstance.getInstanceId(), masterPartition);
                                return;
                            }

                            if (replicaInstance.isStable()) {
//                                if the replica is in the new instance and is stable, we can revoke a master
                                revokeMasterAssignment(instance.getInstanceId(), masterPartition);
                            }
                        });

                instanceAssignmentContainer.getReplicaPartitionSet(instance.getInstanceId())
                        .stream()
                        .forEach(replicaPartition -> revokeReplicaAssignment(instance.getInstanceId(), replicaPartition));

            } else {
//                if there is no new instance, it most likely means that no deployment is happening,
//                and the terminating instance is either restarted or descaled
//                either way, all assignments need to be removed from it if possible
                instanceAssignmentContainer.getReplicaPartitionSet(instance.getInstanceId())
                        .stream()
                        .forEach(replicaPartition -> revokeReplicaAssignment(instance.getInstanceId(), replicaPartition));

                instanceAssignmentContainer.getMasterPartitionSet(instance.getInstanceId())
                        .stream()
                        .forEach(masterPartition -> {
                            Optional<InstanceData> replicaInstance = getReadyReplicaInstance(masterPartition);
                            if (replicaInstance.isPresent() && replicaInstance.get().isStable()) {
//                                if the replica is ready we can revoke the master
                                revokeMasterAssignment(instance.getInstanceId(), masterPartition);
                            }
                        });
            }
        }
    }

    private void optimiseAssignment() {
//        We want to do one optimisation at a time to make sure reassignments keep the state consistent
        if (tryOptimiseMasterAssignments()) {
            return;
        }

        tryOptimiseReplicaAssignments();
    }

    private boolean tryOptimiseMasterAssignments() {
        TreeSet<InstanceData> sortedInstances = getInstancesForOptimisationSortedFromLeastToMostMastersAndAssignmentsTree();
        if (sortedInstances.isEmpty()) {
            return false;
        }

        AssignmentCount leastAssigned = sortedInstances.getFirst().getAssignmentCount();
        Iterator<InstanceData> instanceCountsDescending = sortedInstances.descendingIterator();

        Integer prevMasterCount = -1;
        Integer prevMostOverworkedReplicaCount;
        Map.Entry<String, Integer> prevMostAssignedReplicaInstance = null;
        while (instanceCountsDescending.hasNext()) {
            AssignmentCount instanceCount = instanceCountsDescending.next().getAssignmentCount();

//            If the previous instance has a larger master imbalance then this one, then we should resolve it first,
//            but if the imbalance is the same, then maybe a master from this instance could be optimised faster
            if (instanceCount.getMasterCounter() < prevMasterCount && prevMostAssignedReplicaInstance != null) {
//                To resolve previous master imbalance, we need to move the most overworked replica first, so we unassign it
                revokeReplicaAssignment(prevMostAssignedReplicaInstance.getKey(), prevMostAssignedReplicaInstance.getValue());
                return true;
            }

            if (instanceCount.getMasterCounter() - leastAssigned.getMasterCounter() <= 1) {
//                If there is no imbalance on this instance, then we can safely break the while loop as the instances are sorted,
//                so the following instances also won't have imbalances
                break;
            }

            prevMostOverworkedReplicaCount = -1;
            BitSet masterPartitionsSet = instanceAssignmentContainer.getMasterPartitionSet(instanceCount.getInstance());
            for (int partition = masterPartitionsSet.nextSetBit(0); partition >= 0; partition = masterPartitionsSet.nextSetBit(partition + 1)) {
                Optional<String> replicaInstanceForPartition = partitionAssignmentContainer.getReplicaInstanceForPartition(partition);
                if (replicaInstanceForPartition.isPresent()) {
                    AssignmentCount replicaCount = instanceAssignmentContainer.getCount(replicaInstanceForPartition.get());
                    if (instanceCount.getMasterCounter() - replicaCount.getMasterCounter() > 1) {
//                        If the replica instance has at least 2 masters less, then we can safely move this master there
                        revokeMasterAssignment(instanceCount.getInstance(), partition);
                        return true;
                    } else if (replicaCount.getAssignmentCounter() > prevMostOverworkedReplicaCount) {
//                        But if not, then lets find a replica instance that is the most overworked and see if it needs to be moved in the next iteration
                        prevMostOverworkedReplicaCount = replicaCount.getAssignmentCounter();
                        prevMostAssignedReplicaInstance = entry(replicaCount.getInstance(), partition);
                    }
                } else {
                    log.error("Replica partition {} is not assigned anywhere even though it should have been at this stage", partition);
                }
            }

            prevMasterCount = instanceCount.getMasterCounter();
        }

        return false;
    }

    private void tryOptimiseReplicaAssignments() {
        TreeSet<InstanceData> sortedInstances = getInstancesForOptimisationSortedFromLeastToMostAssignmentsAndMastersTree();
        if (sortedInstances.isEmpty()) {
            return;
        }

        AssignmentCount leastAssigned = sortedInstances.getFirst().getAssignmentCount();

        Iterator<InstanceData> instanceCountsDescending = sortedInstances.descendingIterator();

        while (instanceCountsDescending.hasNext()) {
            AssignmentCount instanceCount = instanceCountsDescending.next().getAssignmentCount();
            int assignmentImbalance = instanceCount.getReplicaCounter() - leastAssigned.getReplicaCounter();
            if (assignmentImbalance < 1) {
//                If there is no imbalance on this instance, then we can safely break the while loop as the instances are sorted,
//                so the following instances also won't have imbalances
                break;
            }

            if (assignmentImbalance == 1 && instanceCount.getMasterCounter() < leastAssigned.getMasterCounter()) {
//                fixme this is the place to potentially change if we go with the shutdown hook
//                If the imbalance is just 1, but the master count in the least assigned is higher, then ignore as we don't want to
//                put more work on instances with more masters
                continue;
            }

//            Find the first replica that could be assigned to the least assigned instance (it can't have any masters there)
            BitSet instanceMasterPartitions = instanceAssignmentContainer.getMasterPartitionSet(leastAssigned.getInstance());
            BitSet instanceReplicaPartitions = instanceAssignmentContainer.getReplicaPartitionSet(instanceCount.getInstance());
            OptionalInt replicaPartition = instanceReplicaPartitions
                    .stream()
                    .filter(p -> !instanceMasterPartitions.get(p))
                    .findFirst();

            if (replicaPartition.isPresent()) {
                revokeReplicaAssignment(instanceCount.getInstance(), replicaPartition.getAsInt());
                return;
            }
        }
    }

    private void revokeMasterAssignment(String instance, Integer masterPartition) {
        partitionAssignmentContainer.removeMasterPartition(masterPartition);
        instanceAssignmentContainer.removeMasterPartition(instance, masterPartition);
    }

    private void revokeReplicaAssignment(String instance, Integer replicaPartition) {
        partitionAssignmentContainer.removeReplicaPartition(replicaPartition);
        instanceAssignmentContainer.removeReplicaPartition(instance, replicaPartition);
    }

    private void forceReplicaAssignment(String instance, Integer replicaPartitionToBeReplaced, Integer replicaPartition) {
        log.info("Revoking replica partition {} from {} instance to replace it with replica partition {}", replicaPartitionToBeReplaced, instance, replicaPartition);
        revokeReplicaAssignment(instance, replicaPartitionToBeReplaced);
        addReplicaAssignment(instance, replicaPartition);
    }

    private PriorityQueue<InstanceData> getNonTerminatingInstancesSortedFromLeastToMostMastersAndAssignmentsQueue() {
        return getNonTerminatingInstances()
                .collect(Collectors.toCollection(
                        () -> new PriorityQueue<>(InstanceData.masterThenAssignmentCountComparator()))
                );
    }

    private TreeSet<InstanceData> getInstancesForOptimisationSortedFromLeastToMostMastersAndAssignmentsTree() {
        return getNonTerminatingInstances()
                .filter(InstanceData::isStable)
                .filter(instanceData -> !instanceData.isNew())
                .collect(Collectors.toCollection(
                        () -> new TreeSet<>(InstanceData.masterThenAssignmentCountComparator()))
                );
    }

    private TreeSet<InstanceData> getInstancesForOptimisationSortedFromLeastToMostAssignmentsAndMastersTree() {
        return getNonTerminatingInstances()
                .filter(InstanceData::isStable)
                .filter(instanceData -> !instanceData.isNew())
                .collect(Collectors.toCollection(
                        () -> new TreeSet<>(InstanceData.assignmentThenMasterCountComparator()))
                );
    }

    private Stream<InstanceData> getNonTerminatingInstances() {
        return instanceAssignmentContainer.getInstanceDataValues()
                .stream()
                .filter(InstanceData::isNotTerminating);
    }

    private Stream<InstanceData> getTerminatingInstances() {
        return instanceAssignmentContainer.getInstanceDataValues()
                .stream()
                .filter(InstanceData::isTerminating);
    }

    private void promoteReplicaToMaster(String instance, Integer partition) {
        log.info("Promoting {} instance from replica to master of {} partition", instance, partition);
        partitionAssignmentContainer.promoteReplicaToMaster(instance, partition);
        instanceAssignmentContainer.promoteReplicaToMaster(instance, partition);
    }
}
