package com.pg.replication.consumer.kafka.assignor;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

import static java.util.Map.entry;

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
            addMasterAssignment(instance, topicPartition);
        } else if (replicaTopic.equals(topicPartition.topic())) {
            addReplicaAssignment(topicPartition, instance);
        }
    }

    private void addMasterAssignment(String instance, TopicPartition topicPartition) {
        partitionAssignmentContainer.addMasterAssignment(instance, topicPartition.partition());
        instanceAssignmentContainer.addMasterAssignment(instance, topicPartition.partition());
    }

    private void addReplicaAssignment(TopicPartition topicPartition, String instance) {
        partitionAssignmentContainer.addReplicaAssignment(instance, topicPartition.partition());
        instanceAssignmentContainer.addReplicaAssignment(instance, topicPartition.partition());
    }

    public Map<String, ConsumerPartitionAssignor.Assignment> assign() {
        if (partitionAssignmentContainer.hasPendingAssignments()) {
//            If there are partitions that need assignment, assign them and return. Optimisations can be done in the next rebalance
            assignPendingPartitions();
        } else {
//            If all partitions are already assigned, we can see if there are any optimisations necessary. We will do them one at a time
//            to make sure reassignments keep the state consistent
            optimiseAssignment();
        }

        return instanceAssignmentContainer.getAssignmentsByConsumer(masterTopic, replicaTopic);
    }
//
    private void optimiseAssignment() {
        boolean isMasterOptimised = tryOptimiseMasterAssignments();
        if (!isMasterOptimised) {
            tryOptimiseReplicaAssignments();
        }
    }

    private boolean tryOptimiseMasterAssignments() {
        TreeSet<InstanceAssignmentContainer.InstanceAssignmentCount> sortedInstances = getInstanceCountsSortedFromLeastToMostMastersAndAssignments();
        InstanceAssignmentContainer.InstanceAssignmentCount leastAssigned = sortedInstances.getFirst();
        Iterator<InstanceAssignmentContainer.InstanceAssignmentCount> instanceCountsDescending = sortedInstances.descendingIterator();

        Integer prevMasterCount = -1;
        Integer prevMostOverworkedReplicaCount = -1;
        Map.Entry<String, Integer> prevMostAssignedReplicaInstance = null;
        while (instanceCountsDescending.hasNext()) {
            InstanceAssignmentContainer.InstanceAssignmentCount instanceCount = instanceCountsDescending.next();

//            If the previous instance has a larger master imbalance then this one, then we should resolve it first,
//            but if the imbalance is the same, then maybe a master from this instance could be optimised faster
            if (instanceCount.getMasterCounter() < prevMasterCount && prevMostAssignedReplicaInstance != null) {
//                fixme do we treat any master optimisation from following steps as better then the replica optimisation, if so, then we can move this after the while loop
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
                    InstanceAssignmentContainer.InstanceAssignmentCount replicaCount = instanceAssignmentContainer.getCount(replicaInstanceForPartition.get());
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
//                    fixme log that this master has no replica assigned and there were no assignments to be done, so sth is wrong
                }
            }

            prevMasterCount = instanceCount.getMasterCounter();
        }

        return false;
    }

    private void tryOptimiseReplicaAssignments() {
        TreeSet<InstanceAssignmentContainer.InstanceAssignmentCount> sortedInstances = getInstanceCountsSortedFromLeastToMostAssignmentsAndMasters();
        InstanceAssignmentContainer.InstanceAssignmentCount leastAssigned = sortedInstances.getFirst();

        Iterator<InstanceAssignmentContainer.InstanceAssignmentCount> instanceCountsDescending = sortedInstances.descendingIterator();

        while (instanceCountsDescending.hasNext()) {
            InstanceAssignmentContainer.InstanceAssignmentCount instanceCount = instanceCountsDescending.next();
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
                revokeReplicaAssignment(instanceCount.getInstance(), replicaPartition.getAsInt());
                return;
            }
        }
    }

    private void revokeMasterPartition(String instance, Integer masterPartition) {
        partitionAssignmentContainer.removeMasterPartition(masterPartition);
        instanceAssignmentContainer.removeMasterPartition(instance, masterPartition);
    }

    private void revokeReplicaAssignment(String instance, Integer replicaPartition) {
        partitionAssignmentContainer.removeReplicaPartition(replicaPartition);
        instanceAssignmentContainer.removeReplicaPartition(instance, replicaPartition);
    }

    private void assignPendingPartitions() {
        Queue<Integer> mastersWithoutReplicasToAssign = new LinkedList<>();
        Queue<Integer> replicaPartitionsToAssign = partitionAssignmentContainer.getReplicaPartitionsToAssign();

        BitSet masterPartitionsToAssign = (BitSet) partitionAssignmentContainer.getMasterPartitionsToAssign().clone();
        for (int masterPartition = masterPartitionsToAssign.nextSetBit(0);
             masterPartition >= 0;
             masterPartition = masterPartitionsToAssign.nextSetBit(masterPartition + 1)) {
            Optional<String> masterCandidateFromReplica = partitionAssignmentContainer.getReplicaInstanceForPartition(masterPartition);
            if (masterCandidateFromReplica.isPresent()) {
                promoteReplicaToMaster(masterPartition, masterCandidateFromReplica.get());
            } else {
                mastersWithoutReplicasToAssign.add(masterPartition);
            }
        }

        SortedSet<InstanceAssignmentContainer.InstanceAssignmentCount> instanceCountsSortedFromLeastToMostAssigned = getInstanceCountsSortedFromLeastToMostMastersAndAssignments();
        Iterator<InstanceAssignmentContainer.InstanceAssignmentCount> instancesIterator = instanceCountsSortedFromLeastToMostAssigned.iterator();
        int averageMastersPerInstance = getAverageMastersPerInstance();
        int averageReplicasPerInstance = getAverageReplicasPerInstance();

        Set<Integer> replicasUnableToAssignToPrevInstance = new HashSet<>();
        while (instancesIterator.hasNext()) {
            InstanceAssignmentContainer.InstanceAssignmentCount instanceCount = instancesIterator.next();

//            If there are masters to assign, assign them up until the average number of masters that should be assigned to every instance for an even split
            while (!mastersWithoutReplicasToAssign.isEmpty()) {
                if (instanceCount.getMasterCounter() >= averageMastersPerInstance) {
                    break;
                }

                Integer masterPartition = mastersWithoutReplicasToAssign.poll();

                if (instanceCount.canIncrement()) {
//                    If space available, just assign a master to the instance
                    addMasterAssignment(instanceCount.getInstance(), new TopicPartition(masterTopic, masterPartition));
                } else if (instanceCount.getReplicaCounter() > 0) {
//                    If no more space on the instance, remove one of replicas and assign a master in its place
                    forceMasterAssignment(new TopicPartition(masterTopic, masterPartition), instanceCount.getInstance());
                }
            }

//            If there's still capacity on the instance, fill it with replicas
            replicaPartitionsToAssign.addAll(replicasUnableToAssignToPrevInstance);
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

                Integer replicaPartition = replicaPartitionsToAssign.poll();
                Boolean isInstanceMasterOfThisReplicaPartition = partitionAssignmentContainer.getMasterInstanceForPartition(replicaPartition)
                        .map(master -> master.equals(instanceCount.getInstance()))
                        .orElse(Boolean.FALSE);

                if (!isInstanceMasterOfThisReplicaPartition) {
//                        We can't assign a replica to the instance that already has a master
                    addReplicaAssignment(new TopicPartition(replicaTopic, replicaPartition), instanceCount.getInstance());
                } else if (instancesIterator.hasNext()) {
//                        If there are more instances, this replica can be assigned to another one
                    replicasUnableToAssignToPrevInstance.add(replicaPartition);
                }

            }
//                If nothing more to assign, it is safe to return here
            if (masterPartitionsToAssign.isEmpty() && replicaPartitionsToAssign.isEmpty()) {
                return;
            }
        }

        if (!mastersWithoutReplicasToAssign.isEmpty()) {
//                fixme add warning logs
        }

        if (!replicaPartitionsToAssign.isEmpty()) {
//                fixme add warning logs
        }
    }

    private void forceMasterAssignment(TopicPartition masterPartition, String instance) {
        int replicaPartition = instanceAssignmentContainer.getReplicaPartitionSet(instance).nextSetBit(0);
        if (replicaPartition >= 0) {
            revokeReplicaAssignment(instance, replicaPartition);
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

    private TreeSet<InstanceAssignmentContainer.InstanceAssignmentCount> getInstanceCountsSortedFromLeastToMostMastersAndAssignments() {
        TreeSet<InstanceAssignmentContainer.InstanceAssignmentCount> instancesSortedByMasterAndAssignmentCounts = new TreeSet<>(InstanceAssignmentContainer.InstanceAssignmentCount.masterThenAssignmentCountComparator());
        instancesSortedByMasterAndAssignmentCounts.addAll(instanceAssignmentContainer.getInstanceAssignmentCounter().values());
        return instancesSortedByMasterAndAssignmentCounts;
    }

    private TreeSet<InstanceAssignmentContainer.InstanceAssignmentCount> getInstanceCountsSortedFromLeastToMostAssignmentsAndMasters() {
        TreeSet<InstanceAssignmentContainer.InstanceAssignmentCount> instancesSortedByMasterAndAssignmentCounts = new TreeSet<>(InstanceAssignmentContainer.InstanceAssignmentCount.assignmentThenMasterCountComparator());
        instancesSortedByMasterAndAssignmentCounts.addAll(instanceAssignmentContainer.getInstanceAssignmentCounter().values());
        return instancesSortedByMasterAndAssignmentCounts;
    }

    private void promoteReplicaToMaster(Integer partition, String instance) {
        partitionAssignmentContainer.promoteReplicaToMaster(instance, partition);
        instanceAssignmentContainer.promoteReplicaToMaster(instance, partition);
    }
}
