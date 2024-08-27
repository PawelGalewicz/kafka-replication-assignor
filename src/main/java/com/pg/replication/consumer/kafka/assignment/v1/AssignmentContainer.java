package com.pg.replication.consumer.kafka.assignment.v1;

import com.pg.replication.consumer.kafka.assignment.v1.InstanceAssignmentContainer.InstanceAssignmentCount;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

import static java.util.Map.entry;

public class AssignmentContainer {
    private final String masterTopic;
    private final String replicaTopic;
    private final Integer maxAssignmentsPerInstance;
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
        this.maxAssignmentsPerInstance = maxAssignmentsPerInstance;
        this.masterTopicPartitionCount = masterTopicPartitionCount;
        this.replicaTopicPartitionCount = replicaTopicPartitionCount;

        this.instanceAssignmentContainer = new InstanceAssignmentContainer(maxAssignmentsPerInstance);
        this.partitionAssignmentContainer = new PartitionAssignmentContainer(masterTopicPartitionCount, replicaTopicPartitionCount);
    }

    public void addInstanceConsumer(String instance, String consumer, Set<String> topics) {
        if (topics.contains(masterTopic)) {
            instanceAssignmentContainer.addInstanceMaster(instance, consumer);
        } else if (topics.contains(replicaTopic)) {
            instanceAssignmentContainer.addInstanceReplica(instance, consumer);
        }
    }

    public boolean canAddAssignment(String instance) {
        return instanceAssignmentContainer.canAddAssignment(instance);
    }

    public void addAssignment(TopicPartition topicPartition, String instance) {
        if (masterTopic.equals(topicPartition.topic())) {
            addMasterAssignment(topicPartition, instance);
        } else if (replicaTopic.equals(topicPartition.topic())) {
            addReplicaAssignment(topicPartition, instance);
        }
    }

    private void addMasterAssignment(TopicPartition topicPartition, String instance) {
        partitionAssignmentContainer.addMasterAssignment(topicPartition, instance);
        instanceAssignmentContainer.addMasterAssignment(topicPartition, instance);
    }

    private void addReplicaAssignment(TopicPartition topicPartition, String instance) {
        partitionAssignmentContainer.addReplicaAssignment(topicPartition, instance);
        instanceAssignmentContainer.addReplicaAssignment(topicPartition, instance);
    }

    public Map<String, ConsumerPartitionAssignor.Assignment> assign() {
        if (partitionAssignmentContainer.hasPendingAssignments()) {
//            If there are partitions that need assignment, assign them and return. Optimisations can be done in the next rebalance
            assignPendingMasterPartitions(); // A phase 1
            assignPendingReplicaPartitions(); // A phase 2
        } else {
//            If all partitions are already assigned, we can see if there are any optimisations necessary. We will do them one at a time
//            to make sure reassignments keep the state consistent
/*
        fixme: do just one optimisation at a time, will be more rebalances but they aren't too costly,
        we do either phase 3 or phase 4
        we check if any instance has more than 1 assignment more then the least assigned, choose this one
*/
            revokeOverassignedPartitions(); // B phase 3 - masters , phase 4 - replicas
        }

        return instanceAssignmentContainer.getAssignmentsByConsumer();
    }

    private void revokeOverassignedPartitions() {
        int numberOfInstances = instanceAssignmentContainer.getNumberOfInstances();

        int avgMastersPerInstance = Math.ceilDiv(masterTopicPartitionCount, numberOfInstances);
        int avgReplicasPerInstance = Math.ceilDiv(replicaTopicPartitionCount, numberOfInstances);
        int avgTopicsPerInstance = avgReplicasPerInstance + avgMastersPerInstance;
//        fixme min would mean more reassignments but also more balanced assignment, max would mean more imbalance, but less reassignments - we don't do them at all
        int maxAssignmentPerInstance = Math.min(maxAssignmentsPerInstance, avgTopicsPerInstance);

//        fixme master imbalance check might need to be done prior to pending replicas being assigned in case a master would be reassigned to a replica that was just assigned
        Set<Map.Entry<String, TopicPartition>> entriesToReassignForMasterImbalances = findPartitionsToRevokeByInstanceForMasterImbalance(
                masterTopicPartitionCount,
                avgMastersPerInstance,
                maxAssignmentPerInstance
        );

        Set<Integer> revokedMasterPartitions = new HashSet<>(entriesToReassignForMasterImbalances.size());
        for (Map.Entry<String, TopicPartition> entry : entriesToReassignForMasterImbalances) {
            if (masterTopic.equals(entry.getValue().topic())) {
                revokeMasterPartition(entry.getKey(), entry.getValue());
                revokedMasterPartitions.add(entry.getValue().partition());
            } else {
                revokeReplicaPartition(entry.getKey(), entry.getValue());
            }
        }

        Set<Map.Entry<String, TopicPartition>> entriesToReassignForReplicaImbalances = findPartitionsToRevokeByInstanceForReplicaImbalance(
                replicaTopicPartitionCount,
                avgReplicasPerInstance,
                revokedMasterPartitions
        );

        for (Map.Entry<String, TopicPartition> entry : entriesToReassignForReplicaImbalances) {
            revokeReplicaPartition(entry.getKey(), entry.getValue());
        }
    }

    private Set<Map.Entry<String, TopicPartition>> findPartitionsToRevokeByInstanceForMasterImbalance(int numberOfMasterPartitions,
                                                                                                      int avgMastersPerInstance,
                                                                                                      int avgConsumersPerInstance) {

        Iterator<InstanceAssignmentCount> masterAssignmentsIterator = instanceAssignmentContainer.getMasterAssignmentCount().iterator();
        Set<Map.Entry<String, TopicPartition>> entriesToReassign = new HashSet<>();
        while (masterAssignmentsIterator.hasNext()) {
            InstanceAssignmentCount masterCount = masterAssignmentsIterator.next();

            Set<Map.Entry<String, TopicPartition>> masterPartitionsToReassign = new HashSet<>(numberOfMasterPartitions);
            Set<Map.Entry<String, TopicPartition>> replicaPartitionsToReassign = new HashSet<>(numberOfMasterPartitions);
            int partitionsOverAvg = masterCount.getMasterCounter() - avgMastersPerInstance;
            if (partitionsOverAvg <= 0) {
                break;
            }

            for (TopicPartition masterPartition : instanceAssignmentContainer.getMasterPartitions(masterCount.getInstance())) {
                partitionAssignmentContainer.getReplicaInstanceForPartition(masterPartition.partition())
                        .map(instanceAssignmentContainer::getCount)
                        .ifPresent(replicaCount -> {
                            if (replicaCount.getMasterCounter() >= avgMastersPerInstance) {
                                return;
                            }
                            if (replicaCount.getAssignmentCounter() >= avgConsumersPerInstance) {
//                              These masters can't be revoked as their replicas are on overworked instances. Instead, unassign replica, so it can be assigned to less overworked instance first
                                replicaPartitionsToReassign.add(entry(replicaCount.getInstance(), toReplica(masterPartition)));
                                return;
                            }
//                          These masters can be revoked as their replicas are on instances that are not overworked
                            masterPartitionsToReassign.add(entry(masterCount.getInstance(), masterPartition));
                        });
            }

//            Start with masters that can already be reassigned in the next rebalance round and see if it's enough to hit intended avg assignments
            for (Map.Entry<String, TopicPartition> assignment : masterPartitionsToReassign) {
                if (partitionsOverAvg <= 0) {
                    break;
                }

                entriesToReassign.add(assignment);
                partitionsOverAvg -= 1;
            }
//            If not, then revoke replicas to reassign them to a less overworked instance in the next rebalance. These instances will then be chosen as masters in the following rebalance
            for (Map.Entry<String, TopicPartition> assignment : replicaPartitionsToReassign) {
                if (partitionsOverAvg <= 0) {
                    break;
                }

                entriesToReassign.add(assignment);
                partitionsOverAvg -= 1;
            }
        }

        return entriesToReassign;
    }

    private Set<Map.Entry<String, TopicPartition>> findPartitionsToRevokeByInstanceForReplicaImbalance(int numberOfReplicaPartitions,
                                                                                                       int avgReplicasPerInstance,
                                                                                                       Set<Integer> revokedMasterPartitions) {
        Iterator<InstanceAssignmentCount> replicaAssignmentsIterator = instanceAssignmentContainer.getReplicaAssignmentCount().iterator();
        Set<Map.Entry<String, TopicPartition>> entriesToReassign = new HashSet<>(numberOfReplicaPartitions);
        while (replicaAssignmentsIterator.hasNext()) {
            InstanceAssignmentCount assignmentCount = replicaAssignmentsIterator.next();

            int partitionsOverAvg = assignmentCount.getReplicaCounter() - avgReplicasPerInstance;
            if (partitionsOverAvg <= 0) {
                break;
            }

            for (TopicPartition replicaPartition : instanceAssignmentContainer.getReplicaPartitions(assignmentCount.getInstance())) {
                if (partitionsOverAvg <= 0) {
                    break;
                }

//                We don't want to revoke replicas for which we revoked masters already
                if (revokedMasterPartitions.contains(replicaPartition.partition())) {
                    continue;
                }

                entriesToReassign.add(entry(assignmentCount.getInstance(), replicaPartition));
                partitionsOverAvg -= 1;
            }
        }


        return entriesToReassign;
    }

    private void revokeMasterPartition(String instance, TopicPartition masterPartition) {
        partitionAssignmentContainer.removeMasterPartition(masterPartition);
        instanceAssignmentContainer.removeMasterPartition(instance, masterPartition);
    }

    private void revokeReplicaPartition(String instance, TopicPartition replicaPartition) {
        partitionAssignmentContainer.removeReplicaPartition(replicaPartition);
        instanceAssignmentContainer.removeReplicaPartition(instance, replicaPartition);
    }

    private TopicPartition toReplica(TopicPartition master) {
        return new TopicPartition(replicaTopic, master.partition());
    }

    private void assignPendingMasterPartitions() {
        BitSet masterPartitionsToAssign = (BitSet) partitionAssignmentContainer.getMasterPartitionsToAssign().clone();
        for (int masterPartition = masterPartitionsToAssign.nextSetBit(0); masterPartition >= 0; masterPartition = masterPartitionsToAssign.nextSetBit(masterPartition + 1)) {
            Optional<String> masterCandidateFromReplica = partitionAssignmentContainer.getReplicaInstanceForPartition(masterPartition);
            if (masterCandidateFromReplica.isPresent()) {
//                We don't check if we can assign with canAddAssignment() because if replica is assigned we can replace it with a master
//                double check if we can unassign many partitions at a given time
                promoteReplicaToMaster(masterPartition, masterCandidateFromReplica.get());
            } else {
                String leastAssignedInstance = instanceAssignmentContainer.getLeastAssigned();
                if (canAddAssignment(leastAssignedInstance)) {
                    addMasterAssignment(new TopicPartition(masterTopic, masterPartition), leastAssignedInstance);
                } else {
//                fixme master not assigned anywhere, we need to unassign one of replicas from the least overworked
                }
            }
        }
    }

    private void assignPendingReplicaPartitions() {
        BitSet replicaPartitionsToAssign = (BitSet) partitionAssignmentContainer.getReplicaPartitionsToAssign().clone();
        for (int replicaPartition = replicaPartitionsToAssign.nextSetBit(0); replicaPartition >= 0; replicaPartition = replicaPartitionsToAssign.nextSetBit(replicaPartition + 1)) {
            Set<String> replicaCandidates = getInstancesNotAssignedToMaster(replicaPartition);
//            iterate over workers and add to fill capacity
//            could be checking assignments or replicas here
            int finalReplicaPartition = replicaPartition;
            instanceAssignmentContainer.getInstanceAssignmentCount()
                    .stream()
//                    See if the instance is not assigned to master and it has capacity for additional assignment
                    .filter(c -> replicaCandidates.contains(c.getInstance()) && canAddAssignment(c.getInstance()))
                    .findFirst()
                    .map(InstanceAssignmentCount::getInstance)
                    .ifPresent(replica -> addReplicaAssignment(new TopicPartition(replicaTopic, finalReplicaPartition), replica));
//            todo add logging if no replica could be chosen
        }
    }

    private Set<String> getInstancesNotAssignedToMaster(Integer partition) {
        Set<String> instances = new HashSet<>(instanceAssignmentContainer.getInstances());
        partitionAssignmentContainer.getMasterInstanceForPartition(partition)
                .ifPresent(instances::remove);
        return instances;
    }

//    public void assignPendingOtherPartitions() {
//        Set<TopicPartition> otherPartitionsToAssign = Set.copyOf(partitionAssignmentContainer.getOtherPartitionsToAssign());
//        for (TopicPartition topicPartition : otherPartitionsToAssign) {
//            String otherCandidate = instanceAssignmentContainer.getLeastAssigned();
//            addOtherAssignment(topicPartition, otherCandidate, consumer);
//        }
//    }

    private void promoteReplicaToMaster(Integer partition, String instance) {
        partitionAssignmentContainer.promoteReplicaToMaster(instance, partition);
        instanceAssignmentContainer.promoteReplicaToMaster(
                instance,
                new TopicPartition(replicaTopic, partition),
                new TopicPartition(masterTopic, partition)
        );
    }
}
