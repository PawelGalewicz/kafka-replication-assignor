package com.pg.replication.consumer.kafka.assignor;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.pg.replication.consumer.kafka.assignor.TestUtils.AssignmentMapContainsCondition;
import static com.pg.replication.consumer.kafka.assignor.TestUtils.AssignmentMapCountCondition;
import static com.pg.replication.consumer.kafka.assignor.TestUtils.AssignmentMapEmptyCondition;
import static com.pg.replication.consumer.kafka.assignor.TestUtils.MASTER_TOPIC;
import static com.pg.replication.consumer.kafka.assignor.TestUtils.REPLICA_TOPIC;
import static com.pg.replication.consumer.kafka.assignor.TestUtils.assertAssignmentContainsAllInstances;
import static com.pg.replication.consumer.kafka.assignor.TestUtils.assertConsumerAssignment;
import static com.pg.replication.consumer.kafka.assignor.TestUtils.cluster;
import static com.pg.replication.consumer.kafka.assignor.TestUtils.groupSubscription;
import static com.pg.replication.consumer.kafka.assignor.TestUtils.masterPartitions;
import static com.pg.replication.consumer.kafka.assignor.TestUtils.replicaPartitions;
import static com.pg.replication.consumer.lifecycle.ApplicationStateContext.ApplicationState.REPLICATING;
import static com.pg.replication.consumer.lifecycle.ApplicationStateContext.ApplicationState.STABLE;
import static com.pg.replication.consumer.lifecycle.ApplicationStateContext.ApplicationState.TERMINATING;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.allOf;
import static org.assertj.core.api.Assertions.anyOf;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 1, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class UnitTestScenariosTests {

    static Map<String, Object> testConfig = Map.of(
            ReplicationCooperativeAssignorConfig.MASTER_TOPIC, MASTER_TOPIC,
            ReplicationCooperativeAssignorConfig.REPLICA_TOPIC, REPLICA_TOPIC,
            ReplicationCooperativeAssignorConfig.MAX_ASSIGNMENTS_PER_INSTANCE, "1000"
    );

    ReplicationCooperativeAssignor assignor;

    @BeforeEach
    void setup() {
        assignor = new ReplicationCooperativeAssignor();
        assignor.configure(testConfig);
    }

    @Nested
    class BasicAssignmentScenarios {

        @Test
        @DisplayName("one replica and one instance - assign replica")
        void oneReplicaIsAssignedToOnlyInstance() {
//        given
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);
            Instance instanceOne = Instance.emptyInstance();

            Cluster cluster = cluster(0, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne);
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).containsExactlyInAnyOrderElementsOf(replicaPartitions);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).isEmpty();
        }

        @Test
        @DisplayName("multiple replicas and one instance - assign all replicas")
        void allReplicasAreAssignedToOnlyInstance() {
//        given
            Integer replicaPartitionsCount = 100;
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);
            Instance instanceOne = Instance.emptyInstance();

            Cluster cluster = cluster(0, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne);
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).containsExactlyInAnyOrderElementsOf(replicaPartitions);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).isEmpty();
        }

        @Test
        @DisplayName("one replica and multiple instances - assign replica to only one instance")
        void oneReplicaIsAssignedToOnlyOneInstance() {
//        given
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);
            Instance instanceOne = Instance.emptyInstance();
            Instance instanceTwo = Instance.emptyInstance();

            Cluster cluster = cluster(0, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo);
            assertThat(assignment.groupAssignment()).is(anyOf(
                    allOf(new AssignmentMapContainsCondition(instanceTwo.getReplicaConsumer(), replicaPartitions), new AssignmentMapEmptyCondition(instanceOne.getReplicaConsumer())),
                    allOf(new AssignmentMapContainsCondition(instanceOne.getReplicaConsumer(), replicaPartitions), new AssignmentMapEmptyCondition(instanceTwo.getReplicaConsumer()))
            ));
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).isEmpty();
        }

        @Test
        @DisplayName("one master and one instance without a replica - don't assign master")
        void masterIsNotAssignedWithoutAReplica() {
//        given
            Integer masterPartitionsCount = 1;
            Instance instanceOne = Instance.emptyInstance();

            Cluster cluster = cluster(masterPartitionsCount, 0);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).isEmpty();
        }

        @Test
        @DisplayName("one master, one replica and one instance - assign only replica")
        void onlyReplicaAssignedIfOneInstance() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            Instance instanceOne = Instance.emptyInstance();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).containsExactlyInAnyOrderElementsOf(replicaPartitions);
        }

        @Test
        @DisplayName("multiple masters, multiple replicas and one instance - assign only replicas")
        void onlyReplicasAssignedIfOneInstance() {
//        given
            Integer masterPartitionsCount = 100;
            Integer replicaPartitionsCount = 100;
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            Instance instanceOne = Instance.emptyInstance();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).containsExactlyInAnyOrderElementsOf(replicaPartitions);
        }

        @Test
        @DisplayName("one master, replica exists - assign master to previous replica, unassign replica")
        void assignMasterToReplica() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            Instance instanceOne = Instance.builder()
                    .noMasterPartitions()
                    .replicaPartitions(replicaPartitions)
                    .build();

            Instance instanceTwo = Instance.emptyInstance();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactlyInAnyOrderElementsOf(masterPartitions);
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).isEmpty();
        }
    }

    @Nested
    class BalancedAssignmentScenarios {

        @Test
        @DisplayName("same number of replicas and instances - assign replica per instance")
        void replicaIsAssignedPerInstance() {
//        given
            Integer replicaPartitionsCount = 2;
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            Instance instanceOne = Instance.emptyInstance();
            Instance instanceTwo = Instance.emptyInstance();

            Cluster cluster = cluster(0, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo);
            assertThat(assignment.groupAssignment()).is(anyOf(
                    allOf(new AssignmentMapContainsCondition(instanceOne.getReplicaConsumer(), singleton(replicaPartitions.get(0))), new AssignmentMapContainsCondition(instanceTwo.getReplicaConsumer(), singleton(replicaPartitions.get(1)))),
                    allOf(new AssignmentMapContainsCondition(instanceOne.getReplicaConsumer(), singleton(replicaPartitions.get(1))), new AssignmentMapContainsCondition(instanceTwo.getReplicaConsumer(), singleton(replicaPartitions.get(0))))
            ));
        }

        @Test
        @DisplayName("more replicas then instances (even number) - assign replicas evenly")
        void evenReplicasAreAssignedEvenlyBetweenInstances() {
//        given
            Integer replicaPartitionsCount = 4;

            Instance instanceOne = Instance.emptyInstance();
            Instance instanceTwo = Instance.emptyInstance();

            Cluster cluster = cluster(0, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo);
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).hasSize(2);
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).hasSize(2);
        }

        @Test
        @DisplayName("more replicas then instances (odd number) - assign replicas evenly")
        void oddReplicasAreAssignedEvenlyBetweenInstances() {
//        given
            Integer replicaPartitionsCount = 5;

            Instance instanceOne = Instance.emptyInstance();
            Instance instanceTwo = Instance.emptyInstance();

            Cluster cluster = cluster(0, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo);
            assertThat(assignment.groupAssignment()).is(anyOf(
                    allOf(new AssignmentMapCountCondition(instanceOne.getReplicaConsumer(), 3), new AssignmentMapCountCondition(instanceTwo.getReplicaConsumer(), 2)),
                    allOf(new AssignmentMapCountCondition(instanceOne.getReplicaConsumer(), 2), new AssignmentMapCountCondition(instanceTwo.getReplicaConsumer(), 3))
            ));
        }
    }

    @Nested
    class PreviousAssignmentPresentScenarios {

        @Test
        @DisplayName("previous assignment exists, nothing to assign - keep previous assignment")
        void keepPreviousAssignments() {
//        given
            Integer masterPartitionsCount = 2;
            Integer replicaPartitionsCount = 2;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            Instance instanceOne = Instance.builder()
                    .masterPartitions(1)
                    .replicaPartitions(0)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .masterPartitions(0)
                    .replicaPartitions(1)
                    .build();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactlyInAnyOrder(masterPartitions.get(1));
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).containsExactlyInAnyOrder(replicaPartitions.get(0));
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).containsExactlyInAnyOrder(masterPartitions.get(0));
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).containsExactlyInAnyOrder(replicaPartitions.get(1));
        }

        @Test
        @DisplayName("previous assignment exists, partitions to assign - assign partitions and keep previous assignment")
        void addPendingAssignmentsAndKeepPreviousOnes() {
//        given
            Integer masterPartitionsCount = 3;
            Integer replicaPartitionsCount = 3;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            Instance instanceOne = Instance.builder()
                    .masterPartitions(1)
                    .replicaPartitions(0)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .masterPartitions(0)
                    .replicaPartitions(1)
                    .build();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactlyInAnyOrder(masterPartitions.get(1));
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).containsExactlyInAnyOrder(masterPartitions.get(0));
            assertThat(assignment.groupAssignment()).is(
                anyOf(
                    allOf(new AssignmentMapContainsCondition(instanceOne.getReplicaConsumer(), List.of(replicaPartitions.get(0), replicaPartitions.get(2))),
                          new AssignmentMapContainsCondition(instanceTwo.getReplicaConsumer(), singleton(replicaPartitions.get(1)))),

                    allOf(new AssignmentMapContainsCondition(instanceOne.getReplicaConsumer(), singleton(replicaPartitions.get(0))),
                          new AssignmentMapContainsCondition(instanceTwo.getReplicaConsumer(), List.of(replicaPartitions.get(1), replicaPartitions.get(2))))
                )
            );
        }
    }

    @Nested
    class MaxAssignmentsScenarios {

        @Test
        @DisplayName("too many replicas to assign for one instance - assign until max value is reached")
        void tooManyReplicasToAssign() {
//        given
            Integer replicaPartitionsCount = 10;
            int maxCapacity = 8;

            Instance instanceOne = Instance.emptyInstance();

            Cluster cluster = cluster(0, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne);

            givenMaxAssignmentsPerInstance(maxCapacity);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne);
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).hasSize(maxCapacity);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).isEmpty();
        }

        @Test
        @DisplayName("replica for unassigned master and one full instance - force replica assignment")
        void forceReplicaToFullInstance() {
//        given
            Integer masterPartitionsCount = 3;
            Integer replicaPartitionsCount = 3;
            int maxCapacity = 2;

            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .replicaPartitions(1)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .masterPartitions(1)
                    .replicaPartitions(0)
                    .build();


            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo);

            givenMaxAssignmentsPerInstance(maxCapacity);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactlyInAnyOrder(masterPartitions.get(0));
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).containsExactlyInAnyOrder(masterPartitions.get(1));
            assertThat(assignment.groupAssignment()).is(
                anyOf(
                    allOf(new AssignmentMapContainsCondition(instanceOne.getReplicaConsumer(), singleton(replicaPartitions.get(2))),
                          new AssignmentMapContainsCondition(instanceTwo.getReplicaConsumer(), singleton(replicaPartitions.get(0)))),

                    allOf(new AssignmentMapContainsCondition(instanceOne.getReplicaConsumer(), singleton(replicaPartitions.get(1))),
                          new AssignmentMapContainsCondition(instanceTwo.getReplicaConsumer(), singleton(replicaPartitions.get(2))))
                )
            );
        }

        void givenMaxAssignmentsPerInstance(Integer value) {
            Map<String, Object> newConfig = new HashMap<>(testConfig);
            newConfig.put(ReplicationCooperativeAssignorConfig.MAX_ASSIGNMENTS_PER_INSTANCE, value.toString());
            assignor.configure(newConfig);
        }
    }

    @Nested
    class BasicOptimisationLogicScenarios {

        @Test
        @DisplayName("pending assignment - don't do any master optimisations")
        void noMasterOptimisationsWhenPendingAssignment() {
//        given
            Integer masterPartitionsCount = 3;
            Integer replicaPartitionsCount = 3;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);

            Instance instanceOne = Instance.builder()
                    .masterPartitions(0, 1)
                    .replicaPartitions(0)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .noMasterPartitions()
                    .replicaPartitions(1)
                    .build();

            Instance instanceThree = Instance.builder()
                    .noMasterPartitions()
                    .replicaPartitions(2)
                    .build();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo, instanceThree);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactlyInAnyOrder(masterPartitions.get(1), masterPartitions.get(0));
        }

        @Test
        @DisplayName("pending assignment - don't do any replica optimisations")
        void noReplicaOptimisationsWhenPendingAssignment() {
//        given
            Integer masterPartitionsCount = 3;
            Integer replicaPartitionsCount = 3;
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .noReplicaPartitions()
                    .build();

            Instance instanceTwo = Instance.builder()
                    .masterPartitions(1)
                    .noReplicaPartitions()
                    .build();

            Instance instanceThree = Instance.builder()
                    .masterPartitions(2)
                    .replicaPartitions(0, 1)
                    .build();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo, instanceThree);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree);
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).containsExactlyInAnyOrder(replicaPartitions.get(1), replicaPartitions.get(0));
        }

        @Test
        @DisplayName("master optimisation possible - don't do any replica optimisations")
        void noReplicaOptimisationsWhenMasterOptimisations() {
//        given
            Integer masterPartitionsCount = 6;
            Integer replicaPartitionsCount = 6;
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            Instance instanceOne = Instance.builder()
                    .masterPartitions(0, 1, 2)
                    .replicaPartitions(2)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .masterPartitions(3, 4)
                    .replicaPartitions(3, 4, 5)
                    .build();

            Instance instanceThree = Instance.builder()
                    .masterPartitions(5)
                    .replicaPartitions(0, 1)
                    .build();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo, instanceThree);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree);
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).containsExactlyInAnyOrder(replicaPartitions.get(3), replicaPartitions.get(4), replicaPartitions.get(5));
        }

        @Test
        @DisplayName("no master optimisation possible - do replica optimisation")
        void doReplicaOptimisationWhenNoMasterOptimisations() {
//        given
            Integer masterPartitionsCount = 3;
            Integer replicaPartitionsCount = 3;

            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .replicaPartitions(2)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .masterPartitions(1)
                    .noReplicaPartitions()
                    .build();

            Instance instanceThree = Instance.builder()
                    .masterPartitions(2)
                    .replicaPartitions(0, 1)
                    .build();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo, instanceThree);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree);
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).hasSize(1);
        }

        @Test
        @DisplayName("no master or replica optimisation possible - do nothing")
        void doNothingWhenNoOptimisations() {
//        given
            Integer masterPartitionsCount = 3;
            Integer replicaPartitionsCount = 3;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .replicaPartitions(2)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .masterPartitions(1)
                    .replicaPartitions(0)
                    .build();

            Instance instanceThree = Instance.builder()
                    .masterPartitions(2)
                    .replicaPartitions(1)
                    .build();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo, instanceThree);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree);
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).containsExactlyInAnyOrder(replicaPartitions.get(2));
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactlyInAnyOrder(masterPartitions.get(0));

            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).containsExactlyInAnyOrder(replicaPartitions.get(0));
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).containsExactlyInAnyOrder(masterPartitions.get(1));

            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).containsExactlyInAnyOrder(replicaPartitions.get(1));
            assertConsumerAssignment(assignment, instanceThree.getMasterConsumer()).containsExactlyInAnyOrder(masterPartitions.get(2));
        }
    }

    @Nested
    class ReplicaOptimisationScenarios {

//        fixme add more sophisticated tests for replica optimisations
        @Test
        @DisplayName("multiple replicas possible to move - choose the one that can be moved to least assigned instance")
        void choosePartitionThatCanBeMovedToLeastAssigned() {
//        given
            Integer masterPartitionsCount = 3;
            Integer replicaPartitionsCount = 3;
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .replicaPartitions(2)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .masterPartitions(1)
                    .noReplicaPartitions()
                    .build();

            Instance instanceThree = Instance.builder()
                    .masterPartitions(2)
                    .replicaPartitions(0, 1)
                    .build();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo, instanceThree);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree);
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).containsExactlyInAnyOrder(replicaPartitions.get(1));
        }
    }

    @Nested
    class MasterOptimisationScenarios {

//        fixme add more sophisticated tests for master optimisations
        @Test
        @DisplayName("replica instance exists for master optimisation - move master to replica instance and unassign replica")
        void doMasterOptimisationIfPossible() {
//        given
            Integer masterPartitionsCount = 3;
            Integer replicaPartitionsCount = 3;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            Instance instanceOne = Instance.builder()
                    .masterPartitions(1, 2)
                    .replicaPartitions(0)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .noMasterPartitions()
                    .replicaPartitions(2)
                    .build();

            Instance instanceThree = Instance.builder()
                    .masterPartitions(0)
                    .replicaPartitions(1)
                    .build();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo, instanceThree);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree);
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).containsExactlyInAnyOrder(replicaPartitions.get(0));
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactlyInAnyOrder(masterPartitions.get(1));

            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).containsExactlyInAnyOrder(replicaPartitions.get(2));
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).isEmpty();

            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).containsExactlyInAnyOrder(replicaPartitions.get(1));
            assertConsumerAssignment(assignment, instanceThree.getMasterConsumer()).containsExactlyInAnyOrder(masterPartitions.get(0));
        }
    }

    @Nested
    class InstanceStateScenarios {

        @Test
        @DisplayName("terminating master, stable replica - unassign master")
        void unassignTerminatingMasterWithStableReplica() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .noMasterPartitions()
                    .replicaPartitions(0)
                    .state(STABLE)
                    .build();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).containsExactlyInAnyOrder(replicaPartitions.get(0));
        }

        @Test
        @DisplayName("terminating master, replicating replica - keep master assigned")
        void keepTerminatingMasterWithReplicatingReplica() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .noMasterPartitions()
                    .replicaPartitions(0)
                    .state(REPLICATING)
                    .build();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactlyInAnyOrder(masterPartitions.get(0));
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).containsExactlyInAnyOrder(replicaPartitions.get(0));
        }

        @Test
        @DisplayName("terminating master, no replica - keep master assigned")
        void keepTerminatingMasterWithNoReplica() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 0;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);

            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .state(STABLE)
                    .build();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactlyInAnyOrder(masterPartitions.get(0));
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).isEmpty();
        }

        @Test
        @DisplayName("terminating replica - unassign replica")
        void unassignTerminatingReplica() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);

            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .noReplicaPartitions()
                    .state(STABLE)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .noMasterPartitions()
                    .replicaPartitions(0)
                    .state(TERMINATING)
                    .build();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactlyInAnyOrder(masterPartitions.get(0));
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).isEmpty();
        }

        @Test
        @DisplayName("terminating master, stable replica not on new instance - unassign replica")
        void unassignReplicaOfTerminatingMasterWithStableReplicaNotOnNewInstance() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);

            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .noMasterPartitions()
                    .replicaPartitions(0)
                    .state(STABLE)
                    .build();

            Instance instanceThree = Instance.builder()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .state(STABLE)
                    .isNew()
                    .build();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo, instanceThree);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactlyInAnyOrder(masterPartitions.get(0));
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).isEmpty();
        }

        @Test
        @DisplayName("terminating master, stable replica on new instance - unassign replica")
        void unassignTerminatingMasterWithStableReplicaOnNewInstance() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .state(STABLE)
                    .build();

            Instance instanceThree = Instance.builder()
                    .noMasterPartitions()
                    .replicaPartitions(0)
                    .state(STABLE)
                    .isNew()
                    .build();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo, instanceThree);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).containsExactlyInAnyOrder(replicaPartitions.get(0));
        }

        @Test
        @DisplayName("terminating master, no replica and a new instance - keep master assigned")
        void keepTerminatingMasterWithNoReplicaAndNewInstance() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 0;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .state(STABLE)
                    .build();

            Instance instanceThree = Instance.builder()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .state(STABLE)
                    .isNew()
                    .build();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo, instanceThree);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactlyInAnyOrder(masterPartitions.get(0));
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).isEmpty();
        }

        @Test
        @DisplayName("terminating replica, a new instance - unassign replica")
        void unassignTerminatingReplicaWithNewInstance() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);

            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .noReplicaPartitions()
                    .state(STABLE)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .noMasterPartitions()
                    .replicaPartitions(0)
                    .state(TERMINATING)
                    .build();

            Instance instanceThree = Instance.builder()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .state(STABLE)
                    .isNew()
                    .build();

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo, instanceThree);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactlyInAnyOrder(masterPartitions.get(0));
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).isEmpty();
        }
    }
}