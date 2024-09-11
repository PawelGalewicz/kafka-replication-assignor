package com.pg.replication.consumer.kafka.assignor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

import static java.util.Map.entry;

@Slf4j
public class AssignmentContainer {
    private final String masterTopic;
    private final String replicaTopic;
    private final Integer masterTopicPartitionCount;
    private final Integer replicaTopicPartitionCount;

    private final PartitionAssignmentContainer partitionAssignmentContainer;
    private final InstanceAssignmentContainer instanceAssignmentContainer;

    public AssignmentContainer(String masterTopic,
                               String replicaTopic,
                               Integer maxAssignmentsPerInstance,
                               Integer masterTopicPartitionCount,
                               Integer replicaTopicPartitionCount) {
        this.masterTopic = masterTopic;
        this.replicaTopic = replicaTopic;
        this.masterTopicPartitionCount = masterTopicPartitionCount;
        this.replicaTopicPartitionCount = replicaTopicPartitionCount;

        this.instanceAssignmentContainer = new InstanceAssignmentContainer(maxAssignmentsPerInstance, masterTopicPartitionCount, replicaTopicPartitionCount);
        this.partitionAssignmentContainer = new PartitionAssignmentContainer(masterTopicPartitionCount, replicaTopicPartitionCount);
    }

    public void addInstanceConsumer(String instance, String consumer, Set<String> topics) {
        if (topics.contains(masterTopic)) {
            instanceAssignmentContainer.addInstanceMasterConsumer(instance, consumer);
        } else if (topics.contains(replicaTopic)) {
            instanceAssignmentContainer.addInstanceReplicaConsumer(instance, consumer);
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
    private void optimiseAssignment() {
//        We want to do one optimisation at a time to make sure reassignments keep the state consistent
        if (tryOptimiseMasterAssignments()) {
            return;
        }

        tryOptimiseReplicaAssignments();
    }

    private boolean tryOptimiseMasterAssignments() {
        TreeSet<InstanceAssignmentContainer.AssignmentCount> sortedInstances = getInstanceCountsSortedFromLeastToMostMastersAndAssignments();
        InstanceAssignmentContainer.AssignmentCount leastAssigned = sortedInstances.getFirst();
        Iterator<InstanceAssignmentContainer.AssignmentCount> instanceCountsDescending = sortedInstances.descendingIterator();

        Integer prevMasterCount = -1;
        Integer prevMostOverworkedReplicaCount;
        Map.Entry<String, Integer> prevMostAssignedReplicaInstance = null;
        while (instanceCountsDescending.hasNext()) {
            InstanceAssignmentContainer.AssignmentCount instanceCount = instanceCountsDescending.next();

//            If the previous instance has a larger master imbalance then this one, then we should resolve it first,
//            but if the imbalance is the same, then maybe a master from this instance could be optimised faster
            if (instanceCount.getMasterCounter() < prevMasterCount && prevMostAssignedReplicaInstance != null) {
//                fixme do we treat any master optimisation from following steps as better then the replica optimisation, if so, then we can move this after the while loop
//                To resolve previous master imbalance, we need to move the most overworked replica first, so we unassign it
                revokeReplicaPartition(prevMostAssignedReplicaInstance.getKey(), prevMostAssignedReplicaInstance.getValue());
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
                        revokeMasterPartition(instanceCount.getInstance(), partition);
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
//                If the imbalance is just 1, but the master count in the least assigned is higher, then ignore as we don't want to
//                put more work on instances with more masters
                continue;
            }

//            Find the first replica that could be assigned to the least assigned instance (it can't have any masters there)
            BitSet instanceMasterPartitions = instanceAssignmentContainer.getMasterPartitionSet(instanceCount.getInstance());
            BitSet instanceReplicaPartitions = instanceAssignmentContainer.getReplicaPartitionSet(instanceCount.getInstance());
            OptionalInt replicaPartition = instanceReplicaPartitions
                    .stream()
                    .filter(p -> !instanceMasterPartitions.get(p))
                    .findFirst();

            if (replicaPartition.isPresent()) {
                revokeReplicaPartition(instanceCount.getInstance(), replicaPartition.getAsInt());
                return;
            }
        }
    }

    private void revokeMasterPartition(String instance, Integer masterPartition) {
        partitionAssignmentContainer.removeMasterPartition(masterPartition);
        instanceAssignmentContainer.removeMasterPartition(instance, masterPartition);
    }

    private void revokeReplicaPartition(String instance, Integer replicaPartition) {
        partitionAssignmentContainer.removeReplicaPartition(replicaPartition);
        instanceAssignmentContainer.removeReplicaPartition(instance, replicaPartition);
    }

    private void assignPendingPartitions() {
        Queue<Integer> mastersWithoutReplicasToAssign = new LinkedList<>();
        BitSet replicaPartitionsToAssign = partitionAssignmentContainer.getReplicaPartitionsToAssign();

        log.debug("Assigning master partitions");

        BitSet masterPartitionsToAssign = (BitSet) partitionAssignmentContainer.getMasterPartitionsToAssign().clone();
        for (int masterPartition = masterPartitionsToAssign.nextSetBit(0);
             masterPartition >= 0;
             masterPartition = masterPartitionsToAssign.nextSetBit(masterPartition + 1)) {
            Optional<String> masterCandidateFromReplica = partitionAssignmentContainer.getReplicaInstanceForPartition(masterPartition);
            if (masterCandidateFromReplica.isPresent()) {
                promoteReplicaToMaster(masterCandidateFromReplica.get(), masterPartition);
            } else {
                mastersWithoutReplicasToAssign.add(masterPartition);
            }
        }

        log.debug("The following master partitions were not assigned due to no replicas available: {}", mastersWithoutReplicasToAssign);

        log.debug("Assigning replicas and unassigned masters to instances");

        SortedSet<InstanceAssignmentContainer.AssignmentCount> instanceCountsSortedFromLeastToMostAssigned = getInstanceCountsSortedFromLeastToMostMastersAndAssignments();
        Iterator<InstanceAssignmentContainer.AssignmentCount> instancesIterator = instanceCountsSortedFromLeastToMostAssigned.iterator();
        int averageMastersPerInstance = getAverageMastersPerInstance();
        int averageReplicasPerInstance = getAverageReplicasPerInstance();

        Set<Integer> replicasUnableToAssignToPrevInstance = new HashSet<>();
        while (instancesIterator.hasNext()) {
            InstanceAssignmentContainer.AssignmentCount instanceCount = instancesIterator.next();
            String instance = instanceCount.getInstance();

//            If there are masters to assign, assign them up until the average number of masters that should be assigned to every instance for an even split
            while (!mastersWithoutReplicasToAssign.isEmpty()) {
                if (instanceCount.getMasterCounter() >= averageMastersPerInstance) {
                    break;
                }

                Integer masterPartition = mastersWithoutReplicasToAssign.poll();

                if (instanceCount.canIncrement()) {
//                    If space available, just assign a master to the instance
                    addMasterAssignment(instance, masterPartition);
                } else if (instanceCount.getReplicaCounter() > 0) {
//                    If no more space on the instance, remove one of replicas and assign a master in its place
                    forceMasterAssignment(instance, masterPartition);
                }
            }

//            If there's still capacity on the instance, fill it with replicas
            replicasUnableToAssignToPrevInstance.forEach(replicaPartitionsToAssign::set);
            replicasUnableToAssignToPrevInstance.clear();
            while (instanceCount.canIncrement()) {
                if (instanceCount.getReplicaCounter() >= averageReplicasPerInstance) {
//                    Don't assign replicas over the average number
                    break;
                }

                if (replicaPartitionsToAssign.isEmpty()) {
//                    No more replicas to assign
                    break;
                }

                Integer replicaPartition = replicaPartitionsToAssign.nextSetBit(0);
                replicaPartitionsToAssign.clear(replicaPartition);

                Boolean isInstanceMasterOfThisReplicaPartition = partitionAssignmentContainer.getMasterInstanceForPartition(replicaPartition)
                        .map(master -> master.equals(instance))
                        .orElse(Boolean.FALSE);

                if (!isInstanceMasterOfThisReplicaPartition) {
//                    We can't assign a replica to the instance that already has a master of that same partition
                    addReplicaAssignment(instance, replicaPartition);
                } else {
//                    If that is the case, this replica can be assigned to the next instance in the iterator
                    replicasUnableToAssignToPrevInstance.add(replicaPartition);
                }

            }
//            If nothing more to assign, it is safe to return here
            if (masterPartitionsToAssign.isEmpty() && replicaPartitionsToAssign.isEmpty()) {
                return;
            }
        }

        if (!mastersWithoutReplicasToAssign.isEmpty()) {
            log.warn("The following master partitions were not assigned anywhere: [{}]", mastersWithoutReplicasToAssign);
        }

        if (!replicaPartitionsToAssign.isEmpty() || !replicasUnableToAssignToPrevInstance.isEmpty()) {
            replicasUnableToAssignToPrevInstance.forEach(replicaPartitionsToAssign::set);
            log.warn("The following replica partitions were not assigned anywhere: [{}]", replicaPartitionsToAssign);
        }
    }

    private void forceMasterAssignment(String instance, Integer masterPartition) {
        int replicaPartition = instanceAssignmentContainer.getReplicaPartitionSet(instance).nextSetBit(0);
        if (replicaPartition >= 0) {
            log.info("Revoking replica partition {} from {} instance to assign master partition {}", replicaPartition, instance, masterPartition);
            revokeReplicaPartition(instance, replicaPartition);
            addMasterAssignment(instance, masterPartition);
        }
    }

    private int getAverageMastersPerInstance() {
        int numberOfInstances = instanceAssignmentContainer.getNumberOfInstances();
        return Math.ceilDiv(masterTopicPartitionCount, numberOfInstances);
    }

    private int getAverageReplicasPerInstance() {
        int numberOfInstances = instanceAssignmentContainer.getNumberOfInstances();
        return Math.ceilDiv(replicaTopicPartitionCount, numberOfInstances);
    }

    private TreeSet<InstanceAssignmentContainer.AssignmentCount> getInstanceCountsSortedFromLeastToMostMastersAndAssignments() {
        TreeSet<InstanceAssignmentContainer.AssignmentCount> instancesSortedByMasterAndAssignmentCounts = new TreeSet<>(InstanceAssignmentContainer.AssignmentCount.masterThenAssignmentCountComparator());
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
