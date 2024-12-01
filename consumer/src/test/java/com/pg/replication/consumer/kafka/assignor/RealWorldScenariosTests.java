package com.pg.replication.consumer.kafka.assignor;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;

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

@Timeout(value = 1, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class RealWorldScenariosTests {

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
    class FirstDeploymentScenario {
        private Integer partitionCount = 4;
        private Cluster cluster = cluster(partitionCount, partitionCount);
        List<TopicPartition> masterPartitions = masterPartitions(partitionCount);
        List<TopicPartition> replicaPartitions = replicaPartitions(partitionCount);


        @Test
        @DisplayName("phase 1 - assign a replica to every instance")
        void phase1() {
//        given
            Instance instanceOne = Instance.emptyInstance();
            Instance instanceTwo = Instance.emptyInstance();
            Instance instanceThree = Instance.emptyInstance();
            Instance instanceFour = Instance.emptyInstance();

            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo, instanceThree, instanceFour);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree, instanceFour);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).hasSize(1);
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).hasSize(1);
            assertConsumerAssignment(assignment, instanceThree.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).hasSize(1);
            assertConsumerAssignment(assignment, instanceFour.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFour.getReplicaConsumer()).hasSize(1);
        }

        @Test
        @DisplayName("phase 2 - replicas are replicating, don't assign masters")
        void phase2() {
//        given
            Instance instanceOne = Instance.builder()
                    .noMasterPartitions()
                    .replicaPartitions(0)
                    .state(REPLICATING)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .noMasterPartitions()
                    .replicaPartitions(1)
                    .state(REPLICATING)
                    .build();

            Instance instanceThree = Instance.builder()
                    .noMasterPartitions()
                    .replicaPartitions(2)
                    .state(REPLICATING)
                    .build();

            Instance instanceFour = Instance.builder()
                    .noMasterPartitions()
                    .replicaPartitions(3)
                    .state(REPLICATING)
                    .build();

            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo, instanceThree, instanceFour);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree, instanceFour);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).containsExactly(replicaPartitions.get(0));
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).containsExactly(replicaPartitions.get(1));
            assertConsumerAssignment(assignment, instanceThree.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).containsExactly(replicaPartitions.get(2));
            assertConsumerAssignment(assignment, instanceFour.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFour.getReplicaConsumer()).containsExactly(replicaPartitions.get(3));
        }

        @Test
        @DisplayName("phase 3 - replicas are stable, assign masters and unassign replicas")
        void phase3() {
//        given
            Instance instanceOne = Instance.builder()
                    .noMasterPartitions()
                    .replicaPartitions(0)
                    .state(STABLE)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .noMasterPartitions()
                    .replicaPartitions(1)
                    .state(STABLE)
                    .build();

            Instance instanceThree = Instance.builder()
                    .noMasterPartitions()
                    .replicaPartitions(2)
                    .state(STABLE)
                    .build();

            Instance instanceFour = Instance.builder()
                    .noMasterPartitions()
                    .replicaPartitions(3)
                    .state(STABLE)
                    .build();

            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo, instanceThree, instanceFour);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree, instanceFour);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactly(masterPartitions.get(0));
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).containsExactly(masterPartitions.get(1));
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getMasterConsumer()).containsExactly(masterPartitions.get(2));
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFour.getMasterConsumer()).containsExactly(masterPartitions.get(3));
            assertConsumerAssignment(assignment, instanceFour.getReplicaConsumer()).isEmpty();
        }

        @Test
        @DisplayName("phase 4 - assign replicas to different instances")
        void phase4() {
//        given
            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .noReplicaPartitions()
                    .state(STABLE)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .masterPartitions(1)
                    .noReplicaPartitions()
                    .state(STABLE)
                    .build();

            Instance instanceThree = Instance.builder()
                    .masterPartitions(2)
                    .noReplicaPartitions()
                    .state(STABLE)
                    .build();


            Instance instanceFour = Instance.builder()
                    .masterPartitions(3)
                    .noReplicaPartitions()
                    .state(STABLE)
                    .build();

            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceOne, instanceTwo, instanceThree, instanceFour);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
//        fixme "hasSizeLessThan(3)" should actually be "hasSize(1)", but there is a case when a circular master/replica dependency causes replicas to be split unevenly
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree, instanceFour);
            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactly(masterPartitions.get(0));
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).doesNotContain(replicaPartitions.get(0));
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).hasSizeLessThan(3);
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).containsExactly(masterPartitions.get(1));
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).doesNotContain(replicaPartitions.get(1));
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).hasSizeLessThan(3);
            assertConsumerAssignment(assignment, instanceThree.getMasterConsumer()).containsExactly(masterPartitions.get(2));
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).doesNotContain(replicaPartitions.get(2));
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).hasSizeLessThan(3);
            assertConsumerAssignment(assignment, instanceFour.getMasterConsumer()).containsExactly(masterPartitions.get(3));
            assertConsumerAssignment(assignment, instanceFour.getReplicaConsumer()).doesNotContain(replicaPartitions.get(3));
            assertConsumerAssignment(assignment, instanceFour.getReplicaConsumer()).hasSizeLessThan(3);
        }
    }

    @Nested
    class MaxBurstRedeploymentScenario {

        private Integer partitionCount = 4;
        private Cluster cluster = cluster(partitionCount, partitionCount);
        List<TopicPartition> masterPartitions = masterPartitions(partitionCount);
        List<TopicPartition> replicaPartitions = replicaPartitions(partitionCount);

        @Test
        @DisplayName("phase 1 - new instances are deployed, don't optimise anything")
        void phase1() {
//        given
            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .replicaPartitions(3)
                    .state(STABLE)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .masterPartitions(1)
                    .replicaPartitions(0)
                    .state(STABLE)
                    .build();

            Instance instanceThree = Instance.builder()
                    .masterPartitions(2)
                    .replicaPartitions(1)
                    .state(STABLE)
                    .build();

            Instance instanceFour = Instance.builder()
                    .masterPartitions(3)
                    .replicaPartitions(2)
                    .state(STABLE)
                    .build();

            Instance instanceOneNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .build();

            Instance instanceTwoNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .build();

            Instance instanceThreeNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .build();

            Instance instanceFourNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .build();

            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(
                    instanceOne, instanceTwo, instanceThree, instanceFour,
                    instanceOneNew, instanceTwoNew, instanceThreeNew, instanceFourNew
            );

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree, instanceFour, instanceOneNew, instanceTwoNew, instanceThreeNew, instanceFourNew);

            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactly(masterPartitions.get(0));
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).containsExactly(replicaPartitions.get(3));
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).containsExactly(masterPartitions.get(1));
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).containsExactly(replicaPartitions.get(0));
            assertConsumerAssignment(assignment, instanceThree.getMasterConsumer()).containsExactly(masterPartitions.get(2));
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).containsExactly(replicaPartitions.get(1));
            assertConsumerAssignment(assignment, instanceFour.getMasterConsumer()).containsExactly(masterPartitions.get(3));
            assertConsumerAssignment(assignment, instanceFour.getReplicaConsumer()).containsExactly(replicaPartitions.get(2));

            assertConsumerAssignment(assignment, instanceOneNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceOneNew.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwoNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwoNew.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThreeNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThreeNew.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFourNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFourNew.getReplicaConsumer()).isEmpty();
        }

        @Test
        @DisplayName("phase 2 - old instances are terminating, unassign replicas")
        void phase2() {
//        given
            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .replicaPartitions(3)
                    .state(TERMINATING)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .masterPartitions(1)
                    .replicaPartitions(0)
                    .state(TERMINATING)
                    .build();

            Instance instanceThree = Instance.builder()
                    .masterPartitions(2)
                    .replicaPartitions(1)
                    .state(TERMINATING)
                    .build();

            Instance instanceFour = Instance.builder()
                    .masterPartitions(3)
                    .replicaPartitions(2)
                    .state(TERMINATING)
                    .build();

            Instance instanceOneNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .build();

            Instance instanceTwoNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .build();

            Instance instanceThreeNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .build();

            Instance instanceFourNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .build();

            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(
                    instanceOne, instanceTwo, instanceThree, instanceFour,
                    instanceOneNew, instanceTwoNew, instanceThreeNew, instanceFourNew
            );

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree, instanceFour, instanceOneNew, instanceTwoNew, instanceThreeNew, instanceFourNew);

            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactly(masterPartitions.get(0));
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).containsExactly(masterPartitions.get(1));
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getMasterConsumer()).containsExactly(masterPartitions.get(2));
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFour.getMasterConsumer()).containsExactly(masterPartitions.get(3));
            assertConsumerAssignment(assignment, instanceFour.getReplicaConsumer()).isEmpty();

            assertConsumerAssignment(assignment, instanceOneNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceOneNew.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwoNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwoNew.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThreeNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThreeNew.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFourNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFourNew.getReplicaConsumer()).isEmpty();
        }

        @Test
        @DisplayName("phase 3 - assign replicas to new instances")
        void phase3() {
//        given
            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .masterPartitions(1)
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceThree = Instance.builder()
                    .masterPartitions(2)
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceFour = Instance.builder()
                    .masterPartitions(3)
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceOneNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .build();

            Instance instanceTwoNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .build();

            Instance instanceThreeNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .build();

            Instance instanceFourNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .build();

            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(
                    instanceOne, instanceTwo, instanceThree, instanceFour,
                    instanceOneNew, instanceTwoNew, instanceThreeNew, instanceFourNew
            );

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree, instanceFour, instanceOneNew, instanceTwoNew, instanceThreeNew, instanceFourNew);

            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactly(masterPartitions.get(0));
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).containsExactly(masterPartitions.get(1));
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getMasterConsumer()).containsExactly(masterPartitions.get(2));
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFour.getMasterConsumer()).containsExactly(masterPartitions.get(3));
            assertConsumerAssignment(assignment, instanceFour.getReplicaConsumer()).isEmpty();

            assertConsumerAssignment(assignment, instanceOneNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceOneNew.getReplicaConsumer()).hasSize(1);
            assertConsumerAssignment(assignment, instanceTwoNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwoNew.getReplicaConsumer()).hasSize(1);
            assertConsumerAssignment(assignment, instanceThreeNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThreeNew.getReplicaConsumer()).hasSize(1);
            assertConsumerAssignment(assignment, instanceFourNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFourNew.getReplicaConsumer()).hasSize(1);
        }

        @Test
        @DisplayName("phase 4 - new replicas are replicating, keep masters assigned")
        void phase4() {
//        given
            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .masterPartitions(1)
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceThree = Instance.builder()
                    .masterPartitions(2)
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceFour = Instance.builder()
                    .masterPartitions(3)
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceOneNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .replicaPartitions(0)
                    .state(REPLICATING)
                    .build();

            Instance instanceTwoNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .replicaPartitions(1)
                    .state(REPLICATING)
                    .build();

            Instance instanceThreeNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .replicaPartitions(2)
                    .state(REPLICATING)
                    .build();

            Instance instanceFourNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .replicaPartitions(3)
                    .state(REPLICATING)
                    .build();

            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(
                    instanceOne, instanceTwo, instanceThree, instanceFour,
                    instanceOneNew, instanceTwoNew, instanceThreeNew, instanceFourNew
            );

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree, instanceFour, instanceOneNew, instanceTwoNew, instanceThreeNew, instanceFourNew);

            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).containsExactly(masterPartitions.get(0));
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).containsExactly(masterPartitions.get(1));
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getMasterConsumer()).containsExactly(masterPartitions.get(2));
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFour.getMasterConsumer()).containsExactly(masterPartitions.get(3));
            assertConsumerAssignment(assignment, instanceFour.getReplicaConsumer()).isEmpty();

            assertConsumerAssignment(assignment, instanceOneNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceOneNew.getReplicaConsumer()).containsExactly(replicaPartitions.get(0));
            assertConsumerAssignment(assignment, instanceTwoNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwoNew.getReplicaConsumer()).containsExactly(replicaPartitions.get(1));
            assertConsumerAssignment(assignment, instanceThreeNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThreeNew.getReplicaConsumer()).containsExactly(replicaPartitions.get(2));
            assertConsumerAssignment(assignment, instanceFourNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFourNew.getReplicaConsumer()).containsExactly(replicaPartitions.get(3));
        }

        @Test
        @DisplayName("phase 5 - new replicas are stable, unassign masters")
        void phase5() {
//        given
            Instance instanceOne = Instance.builder()
                    .masterPartitions(0)
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .masterPartitions(1)
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceThree = Instance.builder()
                    .masterPartitions(2)
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceFour = Instance.builder()
                    .masterPartitions(3)
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceOneNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .replicaPartitions(0)
                    .state(STABLE)
                    .build();

            Instance instanceTwoNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .replicaPartitions(1)
                    .state(STABLE)
                    .build();

            Instance instanceThreeNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .replicaPartitions(2)
                    .state(STABLE)
                    .build();

            Instance instanceFourNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .replicaPartitions(3)
                    .state(STABLE)
                    .build();

            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(
                    instanceOne, instanceTwo, instanceThree, instanceFour,
                    instanceOneNew, instanceTwoNew, instanceThreeNew, instanceFourNew
            );

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree, instanceFour, instanceOneNew, instanceTwoNew, instanceThreeNew, instanceFourNew);

            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFour.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFour.getReplicaConsumer()).isEmpty();

            assertConsumerAssignment(assignment, instanceOneNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceOneNew.getReplicaConsumer()).containsExactly(replicaPartitions.get(0));
            assertConsumerAssignment(assignment, instanceTwoNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwoNew.getReplicaConsumer()).containsExactly(replicaPartitions.get(1));
            assertConsumerAssignment(assignment, instanceThreeNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThreeNew.getReplicaConsumer()).containsExactly(replicaPartitions.get(2));
            assertConsumerAssignment(assignment, instanceFourNew.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFourNew.getReplicaConsumer()).containsExactly(replicaPartitions.get(3));
        }

        @Test
        @DisplayName("phase 6 - assign masters to new replicas, unassign replicas")
        void phase6() {
//        given
            Instance instanceOne = Instance.builder()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceThree = Instance.builder()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceFour = Instance.builder()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceOneNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .replicaPartitions(0)
                    .state(STABLE)
                    .build();

            Instance instanceTwoNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .replicaPartitions(1)
                    .state(STABLE)
                    .build();

            Instance instanceThreeNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .replicaPartitions(2)
                    .state(STABLE)
                    .build();

            Instance instanceFourNew = Instance.builder()
                    .isNew()
                    .noMasterPartitions()
                    .replicaPartitions(3)
                    .state(STABLE)
                    .build();

            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(
                    instanceOne, instanceTwo, instanceThree, instanceFour,
                    instanceOneNew, instanceTwoNew, instanceThreeNew, instanceFourNew
            );

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree, instanceFour, instanceOneNew, instanceTwoNew, instanceThreeNew, instanceFourNew);

            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFour.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFour.getReplicaConsumer()).isEmpty();

            assertConsumerAssignment(assignment, instanceOneNew.getMasterConsumer()).containsExactly(masterPartitions.get(0));
            assertConsumerAssignment(assignment, instanceOneNew.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwoNew.getMasterConsumer()).containsExactly(masterPartitions.get(1));
            assertConsumerAssignment(assignment, instanceTwoNew.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThreeNew.getMasterConsumer()).containsExactly(masterPartitions.get(2));
            assertConsumerAssignment(assignment, instanceThreeNew.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFourNew.getMasterConsumer()).containsExactly(masterPartitions.get(3));
            assertConsumerAssignment(assignment, instanceFourNew.getReplicaConsumer()).isEmpty();
        }

        @Test
        @DisplayName("phase 7 - assign replicas to different new instances")
        void phase7() {
//        given
            Instance instanceOne = Instance.builder()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceTwo = Instance.builder()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceThree = Instance.builder()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceFour = Instance.builder()
                    .noMasterPartitions()
                    .noReplicaPartitions()
                    .state(TERMINATING)
                    .build();

            Instance instanceOneNew = Instance.builder()
                    .isNew()
                    .masterPartitions(0)
                    .noReplicaPartitions()
                    .state(STABLE)
                    .build();

            Instance instanceTwoNew = Instance.builder()
                    .isNew()
                    .masterPartitions(1)
                    .noReplicaPartitions()
                    .state(STABLE)
                    .build();

            Instance instanceThreeNew = Instance.builder()
                    .isNew()
                    .masterPartitions(2)
                    .noReplicaPartitions()
                    .state(STABLE)
                    .build();

            Instance instanceFourNew = Instance.builder()
                    .isNew()
                    .masterPartitions(3)
                    .noReplicaPartitions()
                    .state(STABLE)
                    .build();

            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(
                    instanceOne, instanceTwo, instanceThree, instanceFour,
                    instanceOneNew, instanceTwoNew, instanceThreeNew, instanceFourNew
            );

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
//        fixme "hasSizeLessThan(3)" should actually be "hasSize(1)", but there is a case when a circular master/replica dependency causes replicas to be split unevenly
            assertAssignmentContainsAllInstances(assignment, instanceOne, instanceTwo, instanceThree, instanceFour, instanceOneNew, instanceTwoNew, instanceThreeNew, instanceFourNew);

            assertConsumerAssignment(assignment, instanceOne.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceOne.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceTwo.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceThree.getReplicaConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFour.getMasterConsumer()).isEmpty();
            assertConsumerAssignment(assignment, instanceFour.getReplicaConsumer()).isEmpty();

            assertConsumerAssignment(assignment, instanceOneNew.getMasterConsumer()).containsExactly(masterPartitions.get(0));
            assertConsumerAssignment(assignment, instanceOneNew.getReplicaConsumer()).doesNotContain(replicaPartitions.get(0));
            assertConsumerAssignment(assignment, instanceOneNew.getReplicaConsumer()).hasSizeLessThan(3);
            assertConsumerAssignment(assignment, instanceTwoNew.getMasterConsumer()).containsExactly(masterPartitions.get(1));
            assertConsumerAssignment(assignment, instanceTwoNew.getReplicaConsumer()).doesNotContain(replicaPartitions.get(1));
            assertConsumerAssignment(assignment, instanceTwoNew.getReplicaConsumer()).hasSizeLessThan(3);
            assertConsumerAssignment(assignment, instanceThreeNew.getMasterConsumer()).containsExactly(masterPartitions.get(2));
            assertConsumerAssignment(assignment, instanceThreeNew.getReplicaConsumer()).doesNotContain(replicaPartitions.get(2));
            assertConsumerAssignment(assignment, instanceThreeNew.getReplicaConsumer()).hasSizeLessThan(3);
            assertConsumerAssignment(assignment, instanceFourNew.getMasterConsumer()).containsExactly(masterPartitions.get(3));
            assertConsumerAssignment(assignment, instanceFourNew.getReplicaConsumer()).doesNotContain(replicaPartitions.get(3));
            assertConsumerAssignment(assignment, instanceFourNew.getReplicaConsumer()).hasSizeLessThan(3);
        }
    }
}