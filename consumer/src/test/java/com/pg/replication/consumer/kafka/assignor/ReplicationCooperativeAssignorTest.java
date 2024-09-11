package com.pg.replication.consumer.kafka.assignor;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.stream.IntStreams;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Timeout(value = 1, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ReplicationCooperativeAssignorTest {
    private static final String MASTER_TOPIC = "master_topic";
    private static final String REPLICA_TOPIC = "replica_topic";

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
        @DisplayName("one master and one instance - assign master")
        void oneMasterIsAssignedToOnlyInstance() {
//        given
            String instanceOne = randomString();
            Integer masterPartitionsCount = 1;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            ConsumerSubscription masterSubscription = masterConsumer(instanceOne, emptyList());

            Cluster cluster = cluster(masterPartitionsCount, 0);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(masterSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactly(masterSubscription.consumer);
            assertThat(assignment.groupAssignment().get(masterSubscription.consumer).partitions()).containsExactlyInAnyOrderElementsOf(masterPartitions);
        }

        @Test
        @DisplayName("multiple masters and one instance - assign all masters")
        void allMastersAreAssignedToOnlyInstance() {
//        given
            String instanceOne = randomString();
            Integer masterPartitionsCount = 2;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            ConsumerSubscription masterSubscription = masterConsumer(instanceOne, emptyList());

            Cluster cluster = cluster(masterPartitionsCount, 0);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(masterSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactly(masterSubscription.consumer);
            assertThat(assignment.groupAssignment().get(masterSubscription.consumer).partitions()).containsExactlyInAnyOrderElementsOf(masterPartitions);
        }

        @Test
        @DisplayName("one replica and one instance - assign replica")
        void oneReplicaIsAssignedToOnlyInstance() {
//        given
            String instanceOne = randomString();
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);
            ConsumerSubscription replicaSubscription = replicaConsumer(instanceOne, emptyList());

            Cluster cluster = cluster(0, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(replicaSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactly(replicaSubscription.consumer);
            assertThat(assignment.groupAssignment().get(replicaSubscription.consumer).partitions()).containsExactlyInAnyOrderElementsOf(replicaPartitions);
        }

        @Test
        @DisplayName("multiple replicas and one instance - assign all replicas")
        void allReplicasAreAssignedToOnlyInstance() {
//        given
            String instanceOne = randomString();
            Integer replicaPartitionsCount = 100;
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);
            ConsumerSubscription replicaSubscription = replicaConsumer(instanceOne, emptyList());

            Cluster cluster = cluster(0, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(replicaSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactly(replicaSubscription.consumer);
            assertThat(assignment.groupAssignment().get(replicaSubscription.consumer).partitions()).containsExactlyInAnyOrderElementsOf(replicaPartitions);
        }

        @Test
        @DisplayName("one master, one replica and one instance - assign only master")
        void onlyMasterAssignedIfOneInstance() {
//        given
            String instanceOne = randomString();

            Integer masterPartitionsCount = 1;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            ConsumerSubscription masterSubscription = masterConsumer(instanceOne, emptyList());

            Integer replicaPartitionsCount = 1;
            ConsumerSubscription replicaSubscription = replicaConsumer(instanceOne, emptyList());

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(masterSubscription, replicaSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(masterSubscription.consumer, replicaSubscription.consumer);
            assertThat(assignment.groupAssignment().get(masterSubscription.consumer).partitions()).containsExactlyInAnyOrderElementsOf(masterPartitions);
            assertThat(assignment.groupAssignment().get(replicaSubscription.consumer).partitions()).isEmpty();
        }

        @Test
        @DisplayName("multiple masters, multiple replica and one instance - assign only masters")
        void onlyMastersAssignedIfOneInstance() {
//        given
            String instanceOne = randomString();

            Integer masterPartitionsCount = 100;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            ConsumerSubscription masterSubscription = masterConsumer(instanceOne, emptyList());

            Integer replicaPartitionsCount = 100;
            ConsumerSubscription replicaSubscription = replicaConsumer(instanceOne, emptyList());

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(masterSubscription, replicaSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(masterSubscription.consumer, replicaSubscription.consumer);
            assertThat(assignment.groupAssignment().get(masterSubscription.consumer).partitions()).containsExactlyInAnyOrderElementsOf(masterPartitions);
            assertThat(assignment.groupAssignment().get(replicaSubscription.consumer).partitions()).isEmpty();
        }

        @Test
        @DisplayName("one master and multiple instances - assign master to only one instance")
        void oneMasterIsAssignedToOnlyOneInstance() {
//        given
            String instanceOne = randomString();
            String instanceTwo = randomString();
            Integer masterPartitionsCount = 1;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            ConsumerSubscription masterOneSubscription = masterConsumer(instanceOne, emptyList());
            ConsumerSubscription masterTwoSubscription = masterConsumer(instanceTwo, emptyList());

            Cluster cluster = cluster(masterPartitionsCount, 0);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(masterOneSubscription, masterTwoSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(masterOneSubscription.consumer, masterTwoSubscription.consumer);
            assertThat(assignment.groupAssignment()).is(anyOf(
                    allOf(new AssignmentMapContainsCondition(masterOneSubscription.consumer, masterPartitions), new AssignmentMapEmptyCondition(masterTwoSubscription.consumer)),
                    allOf(new AssignmentMapContainsCondition(masterTwoSubscription.consumer, masterPartitions), new AssignmentMapEmptyCondition(masterOneSubscription.consumer))
            ));
        }

        @Test
        @DisplayName("same number of master and instances - assign master per instance")
        void masterIsAssignedPerInstance() {
//        given
            String instanceOne = randomString();
            String instanceTwo = randomString();
            Integer masterPartitionsCount = 2;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            ConsumerSubscription masterOneSubscription = masterConsumer(instanceOne, emptyList());
            ConsumerSubscription masterTwoSubscription = masterConsumer(instanceTwo, emptyList());

            Cluster cluster = cluster(masterPartitionsCount, 0);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(masterOneSubscription, masterTwoSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(masterOneSubscription.consumer, masterTwoSubscription.consumer);
            assertThat(assignment.groupAssignment()).is(anyOf(
                    allOf(new AssignmentMapContainsCondition(masterOneSubscription.consumer, singleton(masterPartitions.get(0))), new AssignmentMapContainsCondition(masterTwoSubscription.consumer, singleton(masterPartitions.get(1)))),
                    allOf(new AssignmentMapContainsCondition(masterOneSubscription.consumer, singleton(masterPartitions.get(1))), new AssignmentMapContainsCondition(masterTwoSubscription.consumer, singleton(masterPartitions.get(0))))
            ));
        }

        @Test
        @DisplayName("more masters then instances (even number) - assign masters evenly")
        void evenMastersAreAssignedEvenlyBetweenInstances() {
//        given
            String instanceOne = randomString();
            String instanceTwo = randomString();
            Integer masterPartitionsCount = 4;
            ConsumerSubscription masterOneSubscription = masterConsumer(instanceOne, emptyList());
            ConsumerSubscription masterTwoSubscription = masterConsumer(instanceTwo, emptyList());

            Cluster cluster = cluster(masterPartitionsCount, 0);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(masterOneSubscription, masterTwoSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(masterOneSubscription.consumer, masterTwoSubscription.consumer);
            assertThat(assignment.groupAssignment()).is(allOf(
                    new AssignmentMapCountCondition(masterOneSubscription.consumer, 2),
                    new AssignmentMapCountCondition(masterTwoSubscription.consumer, 2)
            ));
        }

        @Test
        @DisplayName("more masters then instances (odd number) - assign masters evenly")
        void oddMastersAreAssignedEvenlyBetweenInstances() {
//        given
            String instanceOne = randomString();
            String instanceTwo = randomString();
            Integer masterPartitionsCount = 5;
            ConsumerSubscription masterOneSubscription = masterConsumer(instanceOne, emptyList());
            ConsumerSubscription masterTwoSubscription = masterConsumer(instanceTwo, emptyList());

            Cluster cluster = cluster(masterPartitionsCount, 0);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(masterOneSubscription, masterTwoSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(masterOneSubscription.consumer, masterTwoSubscription.consumer);
            assertThat(assignment.groupAssignment()).is(anyOf(
                    allOf(new AssignmentMapCountCondition(masterOneSubscription.consumer, 3), new AssignmentMapCountCondition(masterTwoSubscription.consumer, 2)),
                    allOf(new AssignmentMapCountCondition(masterOneSubscription.consumer, 2), new AssignmentMapCountCondition(masterTwoSubscription.consumer, 3))
            ));
        }

        @Test
        @DisplayName("more replicas then instances (even number) - assign replicas evenly")
        void evenReplicasAreAssignedEvenlyBetweenInstances() {
//        given
            String instanceOne = randomString();
            String instanceTwo = randomString();
            Integer replicaPartitionsCount = 4;
            ConsumerSubscription replicaOneSubscription = replicaConsumer(instanceOne, emptyList());
            ConsumerSubscription replicaTwoSubscription = replicaConsumer(instanceTwo, emptyList());

            Cluster cluster = cluster(0, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(replicaOneSubscription, replicaTwoSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(replicaOneSubscription.consumer, replicaTwoSubscription.consumer);
            assertThat(assignment.groupAssignment().get(replicaOneSubscription.consumer).partitions()).hasSize(2);
            assertThat(assignment.groupAssignment().get(replicaTwoSubscription.consumer).partitions()).hasSize(2);
        }

        @Test
        @DisplayName("more replicas then instances (odd number) - assign replicas evenly")
        void oddReplicasAreAssignedEvenlyBetweenInstances() {
//        given
            String instanceOne = randomString();
            String instanceTwo = randomString();
            Integer replicaPartitionsCount = 5;
            ConsumerSubscription replicaOneSubscription = replicaConsumer(instanceOne, emptyList());
            ConsumerSubscription replicaTwoSubscription = replicaConsumer(instanceTwo, emptyList());

            Cluster cluster = cluster(0, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(replicaOneSubscription, replicaTwoSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(replicaOneSubscription.consumer, replicaTwoSubscription.consumer);
            assertThat(assignment.groupAssignment()).is(anyOf(
                    allOf(new AssignmentMapCountCondition(replicaOneSubscription.consumer, 3), new AssignmentMapCountCondition(replicaTwoSubscription.consumer, 2)),
                    allOf(new AssignmentMapCountCondition(replicaOneSubscription.consumer, 2), new AssignmentMapCountCondition(replicaTwoSubscription.consumer, 3))
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

            String instance1 = randomString();
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, singletonList(replicaPartitions.get(0)));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, singletonList(masterPartitions.get(1)));

            String instance2 = randomString();
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, singletonList(replicaPartitions.get(1)));
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, singletonList(masterPartitions.get(0)));

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instance1ReplicaSubscription, instance1MasterSubscription, instance2ReplicaSubscription, instance2MasterSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(instance1ReplicaSubscription.consumer, instance1MasterSubscription.consumer, instance2ReplicaSubscription.consumer, instance2MasterSubscription.consumer);
            assertThat(assignment.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(1));
            assertThat(assignment.groupAssignment().get(instance1ReplicaSubscription.consumer).partitions()).containsExactlyInAnyOrder(replicaPartitions.get(0));
            assertThat(assignment.groupAssignment().get(instance2MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(0));
            assertThat(assignment.groupAssignment().get(instance2ReplicaSubscription.consumer).partitions()).containsExactlyInAnyOrder(replicaPartitions.get(1));
        }

        @Test
        @DisplayName("previous assignment exists, partitions to assign - assign partitions and keep previous assignment")
        void addPendingAssignmentsAndKeepPreviousOnes() {
//        given
            Integer masterPartitionsCount = 3;
            Integer replicaPartitionsCount = 3;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = "instance1";
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, singletonList(replicaPartitions.get(0)));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, singletonList(masterPartitions.get(1)));

            String instance2 = "instance2";
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, singletonList(replicaPartitions.get(1)));
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, singletonList(masterPartitions.get(0)));

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instance1ReplicaSubscription, instance1MasterSubscription, instance2ReplicaSubscription, instance2MasterSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(instance1ReplicaSubscription.consumer, instance1MasterSubscription.consumer, instance2ReplicaSubscription.consumer, instance2MasterSubscription.consumer);
            assertThat(assignment.groupAssignment()).is(anyOf(
                    allOf(new AssignmentMapContainsCondition(instance1MasterSubscription.consumer, List.of(masterPartitions.get(1), masterPartitions.get(2))), new AssignmentMapContainsCondition(instance2MasterSubscription.consumer, singleton(masterPartitions.get(0)))),
                    allOf(new AssignmentMapContainsCondition(instance1MasterSubscription.consumer, singleton(masterPartitions.get(1))), new AssignmentMapContainsCondition(instance2MasterSubscription.consumer, List.of(masterPartitions.get(0), masterPartitions.get(2))))
            ));
            assertThat(assignment.groupAssignment()).is(anyOf(
                    allOf(new AssignmentMapContainsCondition(instance1ReplicaSubscription.consumer, List.of(replicaPartitions.get(0), replicaPartitions.get(2))), new AssignmentMapContainsCondition(instance2ReplicaSubscription.consumer, singleton(replicaPartitions.get(1)))),
                    allOf(new AssignmentMapContainsCondition(instance1ReplicaSubscription.consumer, singleton(replicaPartitions.get(0))), new AssignmentMapContainsCondition(instance2ReplicaSubscription.consumer, List.of(replicaPartitions.get(1), replicaPartitions.get(2))))
            ));
        }

        @Test
        @DisplayName("previous assignment exists, partitions to assign - assign partitions and keep previous assignment")
        void xd() {
//        given
            Integer masterPartitionsCount = 5;
            Integer replicaPartitionsCount = 5;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = "instance1";
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, singletonList(replicaPartitions.get(0)));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, singletonList(masterPartitions.get(1)));

            String instance2 = "instance2";
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, List.of(replicaPartitions.get(1), replicaPartitions.get(2)));
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, emptyList());


            String instance3 = "instance3";
            ConsumerSubscription instance3ReplicaSubscription = replicaConsumer(instance3, List.of(replicaPartitions.get(3), replicaPartitions.get(4)));
            ConsumerSubscription instance3MasterSubscription = masterConsumer(instance3, singletonList(masterPartitions.get(0)));

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(
                    instance1ReplicaSubscription, instance1MasterSubscription,
                    instance2ReplicaSubscription, instance2MasterSubscription,
                    instance3ReplicaSubscription, instance3MasterSubscription
            );

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
        }
    }

    @Nested
    class MaxAssignmentsScenarios {

        @Test
        @DisplayName("too many masters and replicas to assign for one instance - assign just masters until max value is reached")
        void tooManyMastersAndReplicasToAssign() {
//        given
            String instanceOne = randomString();
            Integer masterPartitionsCount = 10;
            Integer replicaPartitionsCount = 10;
            int maxCapacity = 8;
            ConsumerSubscription masterSubscription = masterConsumer(instanceOne, emptyList());
            ConsumerSubscription replicaSubscription = replicaConsumer(instanceOne, emptyList());

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(masterSubscription, replicaSubscription);

            givenMaxAssignmentsPerInstance(maxCapacity);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(masterSubscription.consumer, replicaSubscription.consumer);
            assertThat(assignment.groupAssignment().get(masterSubscription.consumer).partitions()).hasSize(maxCapacity);
            assertThat(assignment.groupAssignment().get(replicaSubscription.consumer).partitions()).hasSize(0);
        }

        @Test
        @DisplayName("enough masters and too many replicas to assign for two instances - assign masters evenly and fill the rest with replicas")
        void enoughMastersAndTooManyReplicasToAssign() {
//        given
            Integer masterPartitionsCount = 10;
            Integer replicaPartitionsCount = 10;

            String instance1 = "instance1";
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, emptyList());
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, emptyList());

            String instance2 = "instance2";
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, emptyList());
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, emptyList());


            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(
                    instance1ReplicaSubscription, instance1MasterSubscription,
                    instance2ReplicaSubscription, instance2MasterSubscription
            );

            int maxCapacity = 8;
            givenMaxAssignmentsPerInstance(maxCapacity);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());

            assertThat(assignment.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).hasSize(5);
            assertThat(assignment.groupAssignment().get(instance1ReplicaSubscription.consumer).partitions()).hasSize(3);

            assertThat(assignment.groupAssignment().get(instance2MasterSubscription.consumer).partitions()).hasSize(5);
            assertThat(assignment.groupAssignment().get(instance2ReplicaSubscription.consumer).partitions()).hasSize(3);
        }
    }

    @Nested
    @Disabled
    class ReassignmentScenarios {

        @Test
        @DisplayName("master unassigned, replica exists - assign master to previous replica, unassign replica")
        void assignMasterToReplica() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String replicatingInstance = randomString();
            ConsumerSubscription instanceReplicaSubscription = replicaConsumer(replicatingInstance, replicaPartitions);
            ConsumerSubscription instanceMasterSubscription = masterConsumer(replicatingInstance, emptyList());

            String otherInstance = randomString();
            ConsumerSubscription otherInstanceReplicaSubscription = replicaConsumer(otherInstance, emptyList());
            ConsumerSubscription otherInstanceMasterSubscription = masterConsumer(otherInstance, emptyList());

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instanceReplicaSubscription, instanceMasterSubscription, otherInstanceReplicaSubscription, otherInstanceMasterSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(instanceReplicaSubscription.consumer, instanceMasterSubscription.consumer, otherInstanceReplicaSubscription.consumer, otherInstanceMasterSubscription.consumer);
            assertThat(assignment.groupAssignment().get(instanceMasterSubscription.consumer).partitions()).containsExactlyInAnyOrderElementsOf(masterPartitions);
            assertThat(assignment.groupAssignment().get(instanceReplicaSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(otherInstanceReplicaSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(otherInstanceMasterSubscription.consumer).partitions()).isEmpty();
        }

        @Test
        @DisplayName("Round one: master unassigned, replica assigned to 1 - assign master to 1, unassign replica" +
                     "Round two: master assigned to 1, replica unassigned - assign replica to 2")
        void assignMasterToReplicaAndThenReplicaToOther() {
//        given (round one)
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = randomString();
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, replicaPartitions);
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, emptyList());

            String instance2 = randomString();
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, emptyList());
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, emptyList());

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instance1ReplicaSubscription, instance1MasterSubscription, instance2ReplicaSubscription, instance2MasterSubscription);

//        when (round one)
            ConsumerPartitionAssignor.GroupAssignment assignmentRoundOne = assignor.assign(cluster, groupSubscription);


//        then (round one)
            assertNotNull(assignmentRoundOne);
            assertNotNull(assignmentRoundOne.groupAssignment());
            assertThat(assignmentRoundOne.groupAssignment().keySet()).containsExactlyInAnyOrder(instance1ReplicaSubscription.consumer, instance1MasterSubscription.consumer, instance2ReplicaSubscription.consumer, instance2MasterSubscription.consumer);
            assertThat(assignmentRoundOne.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).containsExactlyInAnyOrderElementsOf(masterPartitions);
            assertThat(assignmentRoundOne.groupAssignment().get(instance1ReplicaSubscription.consumer).partitions()).isEmpty();
            assertThat(assignmentRoundOne.groupAssignment().get(instance2ReplicaSubscription.consumer).partitions()).isEmpty();
            assertThat(assignmentRoundOne.groupAssignment().get(instance2MasterSubscription.consumer).partitions()).isEmpty();

//        given (round two)

            instance1ReplicaSubscription = replicaConsumer(instance1, assignmentRoundOne.groupAssignment().get(instance1ReplicaSubscription.consumer).partitions());
            instance1MasterSubscription = masterConsumer(instance1, assignmentRoundOne.groupAssignment().get(instance1MasterSubscription.consumer).partitions());
            instance2ReplicaSubscription = replicaConsumer(instance2, assignmentRoundOne.groupAssignment().get(instance2ReplicaSubscription.consumer).partitions());
            instance2MasterSubscription = masterConsumer(instance2, assignmentRoundOne.groupAssignment().get(instance2MasterSubscription.consumer).partitions());

            groupSubscription = groupSubscription(instance1ReplicaSubscription, instance1MasterSubscription, instance2ReplicaSubscription, instance2MasterSubscription);

//        when (round two)
            ConsumerPartitionAssignor.GroupAssignment assignmentRoundTwo = assignor.assign(cluster, groupSubscription);

//        then (round two)
            assertNotNull(assignmentRoundTwo);
            assertNotNull(assignmentRoundTwo.groupAssignment());
            assertThat(assignmentRoundTwo.groupAssignment().keySet()).containsExactlyInAnyOrder(instance1ReplicaSubscription.consumer, instance1MasterSubscription.consumer, instance2ReplicaSubscription.consumer, instance2MasterSubscription.consumer);
            assertThat(assignmentRoundTwo.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).containsExactlyInAnyOrderElementsOf(masterPartitions);
            assertThat(assignmentRoundTwo.groupAssignment().get(instance1ReplicaSubscription.consumer).partitions()).isEmpty();
            assertThat(assignmentRoundTwo.groupAssignment().get(instance2ReplicaSubscription.consumer).partitions()).containsExactlyInAnyOrderElementsOf(replicaPartitions);
            assertThat(assignmentRoundTwo.groupAssignment().get(instance2MasterSubscription.consumer).partitions()).isEmpty();
        }
    }

    @Nested
    @Disabled
    class ImbalanceScenarios {
        @Test
        void scenario1() {
//        given
            Integer masterPartitionsCount = 10;
            Integer replicaPartitionsCount = 10;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = "instance1";
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, List.of(replicaPartitions.get(0)));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, List.of(masterPartitions.get(9)));

            String instance2 = "instance2";
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, List.of(replicaPartitions.get(1)));
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, List.of(masterPartitions.get(8)));

            String instance3 = "instance3";
            ConsumerSubscription instance3ReplicaSubscription = replicaConsumer(instance3, List.of(replicaPartitions.get(2)));
            ConsumerSubscription instance3MasterSubscription = masterConsumer(instance3, List.of(masterPartitions.get(7), masterPartitions.get(6)));

            String instance4 = "instance4";
            ConsumerSubscription instance4ReplicaSubscription = replicaConsumer(instance4, List.of(replicaPartitions.get(3), replicaPartitions.get(4), replicaPartitions.get(5)));
            ConsumerSubscription instance4MasterSubscription = masterConsumer(instance4, List.of(masterPartitions.get(5)));

            String instance5 = "instance5";
            ConsumerSubscription instance5ReplicaSubscription = replicaConsumer(instance5, List.of(replicaPartitions.get(6), replicaPartitions.get(7), replicaPartitions.get(8), replicaPartitions.get(9)));
            ConsumerSubscription instance5MasterSubscription = masterConsumer(instance5, List.of(masterPartitions.get(4), masterPartitions.get(3), masterPartitions.get(2), masterPartitions.get(1), masterPartitions.get(0)));


            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(
                    instance1ReplicaSubscription, instance1MasterSubscription,
                    instance2ReplicaSubscription, instance2MasterSubscription,
                    instance3ReplicaSubscription, instance3MasterSubscription,
                    instance4ReplicaSubscription, instance4MasterSubscription,
                    instance5ReplicaSubscription, instance5MasterSubscription
            );

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());

            assertThat(assignment.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(9));
            assertThat(assignment.groupAssignment().get(instance1ReplicaSubscription.consumer).partitions()).containsExactlyInAnyOrder(replicaPartitions.get(0));

            assertThat(assignment.groupAssignment().get(instance2MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(8));
            assertThat(assignment.groupAssignment().get(instance2ReplicaSubscription.consumer).partitions()).containsExactlyInAnyOrder(replicaPartitions.get(1));

            assertThat(assignment.groupAssignment().get(instance3MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(7), masterPartitions.get(6));
            assertThat(assignment.groupAssignment().get(instance3ReplicaSubscription.consumer).partitions()).containsExactlyInAnyOrder(replicaPartitions.get(2));

            assertThat(assignment.groupAssignment().get(instance4MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(5));
            assertThat(assignment.groupAssignment().get(instance4ReplicaSubscription.consumer).partitions()).contains(replicaPartitions.get(5));
            assertThat(assignment.groupAssignment().get(instance4ReplicaSubscription.consumer).partitions()).hasSize(2);

            assertThat(assignment.groupAssignment().get(instance5MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(4), masterPartitions.get(3), masterPartitions.get(2));
            assertThat(assignment.groupAssignment().get(instance5ReplicaSubscription.consumer).partitions()).hasSize(2);
        }

        @Test
        void scenario2() {
//        given
            Integer masterPartitionsCount = 3;
            Integer replicaPartitionsCount = 3;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = randomString();
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, replicaPartitions);
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, List.of());

            String instance2 = randomString();
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, List.of());
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, masterPartitions);

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(
                    instance1ReplicaSubscription, instance1MasterSubscription,
                    instance2ReplicaSubscription, instance2MasterSubscription
            );

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());

            List<TopicPartition> instance2MasterPartitions = assignment.groupAssignment().get(instance2MasterSubscription.consumer).partitions();

            assertThat(instance2MasterPartitions).hasSize(2);
            assertThat(assignment.groupAssignment().get(instance2ReplicaSubscription.consumer).partitions()).hasSize(0);

            List<TopicPartition> replicasForRevokedMasters = masterPartitions.stream()
                    .filter(o -> !instance2MasterPartitions.contains(o))
                    .map(this::toReplica)
                    .toList();

            assertThat(assignment.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).hasSize(0);
            assertThat(assignment.groupAssignment().get(instance1ReplicaSubscription.consumer).partitions()).hasSize(2);
            assertThat(assignment.groupAssignment().get(instance1ReplicaSubscription.consumer).partitions()).containsAll(replicasForRevokedMasters);
        }

        private TopicPartition toReplica(TopicPartition masterPartition) {
            return new TopicPartition(REPLICA_TOPIC, masterPartition.partition());
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
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = randomString();
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, List.of(replicaPartitions.get(0)));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, List.of(masterPartitions.get(1), masterPartitions.get(0)));


            String instance2 = randomString();
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, List.of(replicaPartitions.get(1)));
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, List.of());


            String instance3 = randomString();
            ConsumerSubscription instance3ReplicaSubscription = replicaConsumer(instance3, List.of(replicaPartitions.get(2)));
            ConsumerSubscription instance3MasterSubscription = masterConsumer(instance3, List.of());

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(
                    instance1ReplicaSubscription, instance1MasterSubscription,
                    instance2ReplicaSubscription, instance2MasterSubscription,
                    instance3ReplicaSubscription, instance3MasterSubscription
            );

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(1), masterPartitions.get(0));
        }

        @Test
        @DisplayName("pending assignment - don't do any replica optimisations")
        void noReplicaOptimisationsWhenPendingAssignment() {
//        given
            Integer masterPartitionsCount = 3;
            Integer replicaPartitionsCount = 3;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = randomString();
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, List.of());
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, List.of(masterPartitions.get(0)));


            String instance2 = randomString();
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, List.of());
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, List.of(masterPartitions.get(1)));


            String instance3 = randomString();
            ConsumerSubscription instance3ReplicaSubscription = replicaConsumer(instance3, List.of(replicaPartitions.get(1), replicaPartitions.get(0)));
            ConsumerSubscription instance3MasterSubscription = masterConsumer(instance3, List.of(masterPartitions.get(2)));

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(
                    instance1ReplicaSubscription, instance1MasterSubscription,
                    instance2ReplicaSubscription, instance2MasterSubscription,
                    instance3ReplicaSubscription, instance3MasterSubscription
            );

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().get(instance3ReplicaSubscription.consumer).partitions()).containsExactlyInAnyOrder(replicaPartitions.get(1), replicaPartitions.get(0));
        }

        @Test
        @DisplayName("master optimisation possible - don't do any replica optimisations")
        void noReplicaOptimisationsWhenMasterOptimisations() {
//        given
            Integer masterPartitionsCount = 6;
            Integer replicaPartitionsCount = 6;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = randomString();
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, List.of(replicaPartitions.get(2)));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, List.of(masterPartitions.get(0), masterPartitions.get(1), masterPartitions.get(2)));


            String instance2 = randomString();
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, List.of(replicaPartitions.get(3), replicaPartitions.get(4), replicaPartitions.get(5)));
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, List.of(masterPartitions.get(3), masterPartitions.get(4)));


            String instance3 = randomString();
            ConsumerSubscription instance3ReplicaSubscription = replicaConsumer(instance3, List.of(replicaPartitions.get(0), replicaPartitions.get(1)));
            ConsumerSubscription instance3MasterSubscription = masterConsumer(instance3, List.of(masterPartitions.get(5)));

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(
                    instance1ReplicaSubscription, instance1MasterSubscription,
                    instance2ReplicaSubscription, instance2MasterSubscription,
                    instance3ReplicaSubscription, instance3MasterSubscription
            );

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().get(instance2ReplicaSubscription.consumer).partitions()).containsExactlyInAnyOrder(replicaPartitions.get(3), replicaPartitions.get(4), replicaPartitions.get(5));
        }

        @Test
        @DisplayName("no master optimisation possible - do replica optimisation")
        void doReplicaOptimisationWhenNoMasterOptimisations() {
//        given
            Integer masterPartitionsCount = 3;
            Integer replicaPartitionsCount = 3;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = randomString();
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, List.of(replicaPartitions.get(2)));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, List.of(masterPartitions.get(0)));


            String instance2 = randomString();
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, List.of());
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, List.of(masterPartitions.get(1)));


            String instance3 = randomString();
            ConsumerSubscription instance3ReplicaSubscription = replicaConsumer(instance3, List.of(replicaPartitions.get(0), replicaPartitions.get(1)));
            ConsumerSubscription instance3MasterSubscription = masterConsumer(instance3, List.of(masterPartitions.get(2)));

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(
                    instance1ReplicaSubscription, instance1MasterSubscription,
                    instance2ReplicaSubscription, instance2MasterSubscription,
                    instance3ReplicaSubscription, instance3MasterSubscription
            );

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().get(instance3ReplicaSubscription.consumer).partitions()).hasSize(1);
        }

        @Test
        @DisplayName("no master or replica optimisation possible - do nothing")
        void doNothingWhenNoOptimisations() {
//        given
            Integer masterPartitionsCount = 3;
            Integer replicaPartitionsCount = 3;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = randomString();
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, List.of(replicaPartitions.get(2)));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, List.of(masterPartitions.get(0)));


            String instance2 = randomString();
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, List.of(replicaPartitions.get(0)));
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, List.of(masterPartitions.get(1)));


            String instance3 = randomString();
            ConsumerSubscription instance3ReplicaSubscription = replicaConsumer(instance3, List.of(replicaPartitions.get(1)));
            ConsumerSubscription instance3MasterSubscription = masterConsumer(instance3, List.of(masterPartitions.get(2)));

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(
                    instance1ReplicaSubscription, instance1MasterSubscription,
                    instance2ReplicaSubscription, instance2MasterSubscription,
                    instance3ReplicaSubscription, instance3MasterSubscription
            );

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().get(instance1ReplicaSubscription.consumer).partitions()).containsExactlyInAnyOrder(replicaPartitions.get(2));
            assertThat(assignment.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(0));

            assertThat(assignment.groupAssignment().get(instance2ReplicaSubscription.consumer).partitions()).containsExactlyInAnyOrder(replicaPartitions.get(0));
            assertThat(assignment.groupAssignment().get(instance2MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(1));

            assertThat(assignment.groupAssignment().get(instance3ReplicaSubscription.consumer).partitions()).containsExactlyInAnyOrder(replicaPartitions.get(1));
            assertThat(assignment.groupAssignment().get(instance3MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(2));
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
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = randomString();
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, List.of(replicaPartitions.get(2)));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, List.of(masterPartitions.get(0)));


            String instance2 = randomString();
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, List.of());
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, List.of(masterPartitions.get(1)));


            String instance3 = randomString();
            ConsumerSubscription instance3ReplicaSubscription = replicaConsumer(instance3, List.of(replicaPartitions.get(0), replicaPartitions.get(1)));
            ConsumerSubscription instance3MasterSubscription = masterConsumer(instance3, List.of(masterPartitions.get(2)));

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(
                    instance1ReplicaSubscription, instance1MasterSubscription,
                    instance2ReplicaSubscription, instance2MasterSubscription,
                    instance3ReplicaSubscription, instance3MasterSubscription
            );

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().get(instance3ReplicaSubscription.consumer).partitions()).containsExactlyInAnyOrder(replicaPartitions.get(1));
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

            String instance1 = randomString();
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, List.of(replicaPartitions.get(0)));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, List.of(masterPartitions.get(1), masterPartitions.get(2)));

            String instance2 = randomString();
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, List.of(replicaPartitions.get(2)));
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, List.of());

            String instance3 = randomString();
            ConsumerSubscription instance3ReplicaSubscription = replicaConsumer(instance3, List.of(replicaPartitions.get(1)));
            ConsumerSubscription instance3MasterSubscription = masterConsumer(instance3, List.of(masterPartitions.get(0)));

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(
                    instance1ReplicaSubscription, instance1MasterSubscription,
                    instance2ReplicaSubscription, instance2MasterSubscription,
                    instance3ReplicaSubscription, instance3MasterSubscription
            );

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().get(instance1ReplicaSubscription.consumer).partitions()).containsExactlyInAnyOrder(replicaPartitions.get(0));
            assertThat(assignment.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(1));

            assertThat(assignment.groupAssignment().get(instance2ReplicaSubscription.consumer).partitions()).containsExactlyInAnyOrder(replicaPartitions.get(2));
            assertThat(assignment.groupAssignment().get(instance2MasterSubscription.consumer).partitions()).isEmpty();

            assertThat(assignment.groupAssignment().get(instance3ReplicaSubscription.consumer).partitions()).containsExactlyInAnyOrder(replicaPartitions.get(1));
            assertThat(assignment.groupAssignment().get(instance3MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(0));
        }
    }

    private static String randomString() {
        return RandomStringUtils.randomAlphabetic(10);
    }

    void givenMaxAssignmentsPerInstance(Integer value) {
        Map<String, Object> newConfig = new HashMap<>(testConfig);
        newConfig.put(ReplicationCooperativeAssignorConfig.MAX_ASSIGNMENTS_PER_INSTANCE, value.toString());
        assignor.configure(newConfig);
    }

    ConsumerSubscription masterConsumer(String instance, List<TopicPartition> ownedPartitions) {
        return consumer(instance, MASTER_TOPIC, ownedPartitions);
    }

    ConsumerSubscription replicaConsumer(String instance, List<TopicPartition> ownedPartitions) {
        return consumer(instance, REPLICA_TOPIC, ownedPartitions);
    }

    ConsumerSubscription consumer(String instance, String topic, List<TopicPartition> ownedPartitions) {
        List<String> topics = singletonList(topic);
        AssignmentMetadata assignmentMetadata = new AssignmentMetadata(instance);
        ByteBuffer userData = ReplicationCooperativeAssignor.encodeAssignmentMetadata(assignmentMetadata);
        ConsumerPartitionAssignor.Subscription subscription = new ConsumerPartitionAssignor.Subscription(topics, userData, ownedPartitions);
        return new ConsumerSubscription(instance + "_" + topic + "_consumer", subscription);
    }

    ConsumerPartitionAssignor.GroupSubscription groupSubscription(ConsumerSubscription... subscriptions) {
        Map<String, ConsumerPartitionAssignor.Subscription> subscriptionMap = new HashMap<>();
        for (ConsumerSubscription consumerSubscription : subscriptions) {
            subscriptionMap.put(consumerSubscription.consumer, consumerSubscription.subscription);
        }
        return new ConsumerPartitionAssignor.GroupSubscription(subscriptionMap);
    }

    List<TopicPartition> masterPartitions(Integer partitions) {
        return topicPartitions(MASTER_TOPIC, partitions);
    }

    List<TopicPartition> replicaPartitions(Integer partitions) {
        return topicPartitions(REPLICA_TOPIC, partitions);
    }

    List<TopicPartition> topicPartitions(String topic, Integer partitions) {
        return IntStreams.range(partitions)
                .mapToObj(p -> new TopicPartition(topic, p))
                .collect(Collectors.toList());
    }

    Cluster cluster(Integer masterPartitions, Integer replicaPartitions) {
        String clusterId = randomString();
        Set<PartitionInfo> partitions = new HashSet<>(masterPartitions + replicaPartitions);

        IntStreams.range(masterPartitions)
                .forEach(p -> partitions.add(new PartitionInfo(MASTER_TOPIC, p, null, null, null)));

        IntStreams.range(replicaPartitions)
                .forEach(p -> partitions.add(new PartitionInfo(REPLICA_TOPIC, p, null, null, null)));

        return new Cluster(clusterId, emptySet(), partitions, emptySet(), emptySet());
    }

    record ConsumerSubscription(String consumer, ConsumerPartitionAssignor.Subscription subscription) {}

    static class AssignmentMapContainsCondition extends Condition<Map<String, ConsumerPartitionAssignor.Assignment>> {

        private final String consumerToWhichPartitionsShouldBeAssigned;
        private final Collection<TopicPartition> partitionsThatShouldBeAssigned;

        AssignmentMapContainsCondition(String consumerToWhichPartitionsShouldBeAssigned, Collection<TopicPartition> partitionsThatShouldBeAssigned) {
            this.consumerToWhichPartitionsShouldBeAssigned = consumerToWhichPartitionsShouldBeAssigned;
            this.partitionsThatShouldBeAssigned = partitionsThatShouldBeAssigned;
        }


        @Override
        public boolean matches(Map<String, ConsumerPartitionAssignor.Assignment> map) {
            List<TopicPartition> assignedPartitions = map.get(consumerToWhichPartitionsShouldBeAssigned).partitions();
            boolean containsAll = assignedPartitions.containsAll(partitionsThatShouldBeAssigned);
            List<TopicPartition> assignedPartitionsCopy = new ArrayList<>(assignedPartitions);
            assignedPartitionsCopy.removeAll(partitionsThatShouldBeAssigned);
            boolean containsNothingElse = assignedPartitionsCopy.isEmpty();

            return containsAll && containsNothingElse;
        }
    }

    static class AssignmentMapCountCondition extends Condition<Map<String, ConsumerPartitionAssignor.Assignment>> {

        private final String consumerToWhichPartitionsShouldBeAssigned;
        private final int numberOfPartitionsThatShouldBeAssigned;

        AssignmentMapCountCondition(String consumerToWhichPartitionsShouldBeAssigned, int numberOfPartitionsThatShouldBeAssigned) {
            this.consumerToWhichPartitionsShouldBeAssigned = consumerToWhichPartitionsShouldBeAssigned;
            this.numberOfPartitionsThatShouldBeAssigned = numberOfPartitionsThatShouldBeAssigned;
        }

        @Override
        public boolean matches(Map<String, ConsumerPartitionAssignor.Assignment> map) {
            return map.get(consumerToWhichPartitionsShouldBeAssigned).partitions().size() == numberOfPartitionsThatShouldBeAssigned;
        }
    }

    static class AssignmentMapEmptyCondition extends Condition<Map<String, ConsumerPartitionAssignor.Assignment>> {

        private final String consumerToWhichNoPartitionsShouldBeAssigned;
        AssignmentMapEmptyCondition(String consumerToWhichPartitionsShouldBeAssigned) {
            this.consumerToWhichNoPartitionsShouldBeAssigned = consumerToWhichPartitionsShouldBeAssigned;
        }

        @Override
        public boolean matches(Map<String, ConsumerPartitionAssignor.Assignment> map) {
            return map.get(consumerToWhichNoPartitionsShouldBeAssigned).partitions().isEmpty();
        }
    }
}