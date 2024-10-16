package com.pg.replication.consumer.kafka.assignor;

import com.pg.replication.consumer.lifecycle.ApplicationStateContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

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
                                    ApplicationStateContext.ApplicationState instanceState,
                                    String consumer,
                                    Collection<String> topics) {
        if (topics.contains(masterTopic)) {
            instanceAssignmentContainer.addInstanceMasterConsumer(instance, instanceState, consumer);
        } else if (topics.contains(replicaTopic)) {
            instanceAssignmentContainer.addInstanceReplicaConsumer(instance, instanceState, consumer);
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
            Optional<String> masterCandidateFromReplica = partitionAssignmentContainer.getReplicaInstanceForPartition(masterPartition);
            if (masterCandidateFromReplica.isPresent()) {
                promoteReplicaToMaster(masterCandidateFromReplica.get(), masterPartition);
                masterPartitionsToAssign.clear(masterPartition);
            }
        }

        log.debug("The following master partitions were not assigned due to no replicas available: {}", masterPartitionsToAssign);

        PriorityQueue<InstanceAssignmentContainer.AssignmentCount> sortedInstanceCounts = getInstanceCountsSortedFromLeastToMostMastersAndAssignmentsQueue();

        masterPartitionsToAssign.and(replicaPartitionsToAssign);
        BitSet replicaPartitionsForUnassignedMasters = masterPartitionsToAssign;

        log.debug("Assigning replica partitions for unassigned masters");
        while (!sortedInstanceCounts.isEmpty()) {
            int replicaPartition = replicaPartitionsForUnassignedMasters.nextSetBit(0);
            if (replicaPartition < 0) {
                break;
            }

            InstanceAssignmentContainer.AssignmentCount instanceCount = sortedInstanceCounts.poll();
            String instance = instanceCount.getInstance();
            if (instanceCount.canIncrement()) {
    //            If space available, just assign a replica to the instance
                addReplicaAssignment(instance, replicaPartition);
                sortedInstanceCounts.add(instanceCount);
                replicaPartitionsForUnassignedMasters.clear(replicaPartition);
                replicaPartitionsToAssign.clear(replicaPartition);
            } else if (instanceCount.getReplicaCounter() > 0) {
    //            If no more space on the instance, but there is another replica, replace it as a replica for an unassigned master has higher priority
                OptionalInt replicaPartitionToReplace = instanceAssignmentContainer.getReplicaPartitionSet(instance)
                        .stream()
                        .filter(partition -> !masterPartitionsToAssign.get(partition))
                        .findFirst();

                if (replicaPartitionToReplace.isPresent()) {
                    forceReplicaAssignment(instance, replicaPartitionToReplace.getAsInt(), replicaPartition);
                    replicaPartitionsForUnassignedMasters.clear(replicaPartition);
                    replicaPartitionsToAssign.clear(replicaPartition);
                }
            }

            if (instanceCount.canIncrement()) {
                sortedInstanceCounts.add(instanceCount);
            }
        }

        log.debug("Assigning the rest of replicas");
        InstanceAssignmentContainer.AssignmentCount skippedInstance = null;
        while (!sortedInstanceCounts.isEmpty()) {
            int replicaPartition = replicaPartitionsToAssign.nextSetBit(0);
            if (replicaPartition < 0) {
                break;
            }

            InstanceAssignmentContainer.AssignmentCount instanceCount = sortedInstanceCounts.poll();
            String instance = instanceCount.getInstance();

            if (!instanceCount.canIncrement()) {
                continue;
            }

    //        If we skipped an instance already, then it means we already found a master and don't need to check for it again
            if (skippedInstance == null) {
                Boolean isInstanceMasterOfThisReplicaPartition = partitionAssignmentContainer.getMasterInstanceForPartition(replicaPartition)
                        .map(master -> master.equals(instance))
                        .orElse(Boolean.FALSE);

    //        A replica cannot be assigned to the instance that already has a master of that same partition
                if (isInstanceMasterOfThisReplicaPartition) {
                    skippedInstance = instanceCount;
                    continue;
                }
            }

            addReplicaAssignment(instance, replicaPartition);
            replicaPartitionsToAssign.clear(replicaPartition);


            if (instanceCount.canIncrement()) {
                sortedInstanceCounts.add(instanceCount);
            }

            if (skippedInstance != null && skippedInstance.canIncrement()) {
                sortedInstanceCounts.add(skippedInstance);
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

    private void optimiseAssignment() {
//        We want to do one optimisation at a time to make sure reassignments keep the state consistent
        if (tryOptimiseMasterAssignments()) {
            return;
        }

        tryOptimiseReplicaAssignments();
    }

    private boolean tryOptimiseMasterAssignments() {
        TreeSet<InstanceAssignmentContainer.AssignmentCount> sortedInstances = getInstanceCountsSortedFromLeastToMostMastersAndAssignmentsTree();
        InstanceAssignmentContainer.AssignmentCount leastAssigned = sortedInstances.getFirst();
        Iterator<InstanceAssignmentContainer.AssignmentCount> instanceCountsDescending = sortedInstances.descendingIterator();

        Integer prevMasterCount = -1;
//        fixme save the count object and compare masters and overall assignments when setting it
        Integer prevMostOverworkedReplicaCount;
        Map.Entry<String, Integer> prevMostAssignedReplicaInstance = null;
        while (instanceCountsDescending.hasNext()) {
            InstanceAssignmentContainer.AssignmentCount instanceCount = instanceCountsDescending.next();

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
                    InstanceAssignmentContainer.AssignmentCount replicaCount = instanceAssignmentContainer.getCount(replicaInstanceForPartition.get());
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
        TreeSet<InstanceAssignmentContainer.AssignmentCount> sortedInstances = getInstanceCountsSortedFromLeastToMostAssignmentsAndMasters();
        InstanceAssignmentContainer.AssignmentCount leastAssigned = sortedInstances.getFirst();

        Iterator<InstanceAssignmentContainer.AssignmentCount> instanceCountsDescending = sortedInstances.descendingIterator();

        while (instanceCountsDescending.hasNext()) {
            InstanceAssignmentContainer.AssignmentCount instanceCount = instanceCountsDescending.next();
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

    private TreeSet<InstanceAssignmentContainer.AssignmentCount> getInstanceCountsSortedFromLeastToMostMastersAndAssignmentsTree() {
        TreeSet<InstanceAssignmentContainer.AssignmentCount> instancesSortedByMasterAndAssignmentCounts = new TreeSet<>(InstanceAssignmentContainer.AssignmentCount.masterThenAssignmentCountComparator());
        instancesSortedByMasterAndAssignmentCounts.addAll(instanceAssignmentContainer.getInstanceAssignmentCounterValues());
        return instancesSortedByMasterAndAssignmentCounts;
    }

    private PriorityQueue<InstanceAssignmentContainer.AssignmentCount> getInstanceCountsSortedFromLeastToMostMastersAndAssignmentsQueue() {
        PriorityQueue<InstanceAssignmentContainer.AssignmentCount> instancesSortedByMasterAndAssignmentCounts = new PriorityQueue<>(InstanceAssignmentContainer.AssignmentCount.masterThenAssignmentCountComparator());
        instancesSortedByMasterAndAssignmentCounts.addAll(instanceAssignmentContainer.getInstanceAssignmentCounterValues());
        return instancesSortedByMasterAndAssignmentCounts;
    }

    private TreeSet<InstanceAssignmentContainer.AssignmentCount> getInstanceCountsSortedFromLeastToMostAssignmentsAndMasters() {
        TreeSet<InstanceAssignmentContainer.AssignmentCount> instancesSortedByMasterAndAssignmentCounts = new TreeSet<>(InstanceAssignmentContainer.AssignmentCount.assignmentThenMasterCountComparator());
        instancesSortedByMasterAndAssignmentCounts.addAll(instanceAssignmentContainer.getInstanceAssignmentCounterValues());
        return instancesSortedByMasterAndAssignmentCounts;
    }

    private void promoteReplicaToMaster(String instance, Integer partition) {
        log.info("Promoting {} instance from replica to master of {} partition", instance, partition);
        partitionAssignmentContainer.promoteReplicaToMaster(instance, partition);
        instanceAssignmentContainer.promoteReplicaToMaster(instance, partition);
    }
}
