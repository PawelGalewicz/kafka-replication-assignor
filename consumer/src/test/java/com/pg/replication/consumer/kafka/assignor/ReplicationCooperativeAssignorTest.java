package com.pg.replication.consumer.kafka.assignor;

import com.pg.replication.consumer.lifecycle.ApplicationStateContext;
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

import static com.pg.replication.consumer.lifecycle.ApplicationStateContext.ApplicationState.*;
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
        @DisplayName("one replica and multiple instances - assign replica to only one instance")
        void oneReplicaIsAssignedToOnlyOneInstance() {
//        given
            String instanceOne = randomString();
            String instanceTwo = randomString();
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);
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
                    allOf(new AssignmentMapContainsCondition(replicaOneSubscription.consumer, replicaPartitions), new AssignmentMapEmptyCondition(replicaTwoSubscription.consumer)),
                    allOf(new AssignmentMapContainsCondition(replicaTwoSubscription.consumer, replicaPartitions), new AssignmentMapEmptyCondition(replicaOneSubscription.consumer))
            ));
        }

        @Test
        @DisplayName("one master and one instance without a replica - don't assign master")
        void masterIsNotAssignedWithoutAReplica() {
//        given
            String instanceOne = randomString();
            Integer masterPartitionsCount = 1;
            ConsumerSubscription masterSubscription = masterConsumer(instanceOne, emptyList());

            Cluster cluster = cluster(masterPartitionsCount, 0);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(masterSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactly(masterSubscription.consumer);
            assertThat(assignment.groupAssignment().get(masterSubscription.consumer).partitions()).isEmpty();
        }

        @Test
        @DisplayName("one master, one replica and one instance - assign only replica")
        void onlyReplicaAssignedIfOneInstance() {
//        given
            String instanceOne = randomString();

            Integer masterPartitionsCount = 1;
            ConsumerSubscription masterSubscription = masterConsumer(instanceOne, emptyList());

            Integer replicaPartitionsCount = 1;
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);
            ConsumerSubscription replicaSubscription = replicaConsumer(instanceOne, emptyList());

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(masterSubscription, replicaSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(masterSubscription.consumer, replicaSubscription.consumer);
            assertThat(assignment.groupAssignment().get(masterSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(replicaSubscription.consumer).partitions()).containsExactlyInAnyOrderElementsOf(replicaPartitions);
        }

        @Test
        @DisplayName("multiple masters, multiple replicas and one instance - assign only replicas")
        void onlyReplicasAssignedIfOneInstance() {
//        given
            String instanceOne = randomString();

            Integer masterPartitionsCount = 100;
            ConsumerSubscription masterSubscription = masterConsumer(instanceOne, emptyList());

            Integer replicaPartitionsCount = 100;
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);
            ConsumerSubscription replicaSubscription = replicaConsumer(instanceOne, emptyList());

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(masterSubscription, replicaSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(masterSubscription.consumer, replicaSubscription.consumer);
            assertThat(assignment.groupAssignment().get(masterSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(replicaSubscription.consumer).partitions()).containsExactlyInAnyOrderElementsOf(replicaPartitions);
        }

        @Test
        @DisplayName("one master, replica exists - assign master to previous replica, unassign replica")
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
    }

    @Nested
    class BalancedAssignmentScenarios {

        @Test
        @DisplayName("same number of replicas and instances - assign replica per instance")
        void replicaIsAssignedPerInstance() {
//        given
            String instanceOne = randomString();
            String instanceTwo = randomString();
            Integer replicaPartitionsCount = 2;
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);
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
                    allOf(new AssignmentMapContainsCondition(replicaOneSubscription.consumer, singleton(replicaPartitions.get(0))), new AssignmentMapContainsCondition(replicaTwoSubscription.consumer, singleton(replicaPartitions.get(1)))),
                    allOf(new AssignmentMapContainsCondition(replicaOneSubscription.consumer, singleton(replicaPartitions.get(1))), new AssignmentMapContainsCondition(replicaTwoSubscription.consumer, singleton(replicaPartitions.get(0))))
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
            assertThat(assignment.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(1));
            assertThat(assignment.groupAssignment().get(instance2MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(0));
            assertThat(assignment.groupAssignment()).is(
                anyOf(
                    allOf(new AssignmentMapContainsCondition(instance1ReplicaSubscription.consumer, List.of(replicaPartitions.get(0), replicaPartitions.get(2))),
                          new AssignmentMapContainsCondition(instance2ReplicaSubscription.consumer, singleton(replicaPartitions.get(1)))),

                    allOf(new AssignmentMapContainsCondition(instance1ReplicaSubscription.consumer, singleton(replicaPartitions.get(0))),
                          new AssignmentMapContainsCondition(instance2ReplicaSubscription.consumer, List.of(replicaPartitions.get(1), replicaPartitions.get(2))))
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
            String instanceOne = randomString();
            Integer replicaPartitionsCount = 10;
            int maxCapacity = 8;
            ConsumerSubscription replicaSubscription = replicaConsumer(instanceOne, emptyList());

            Cluster cluster = cluster(0, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(replicaSubscription);

            givenMaxAssignmentsPerInstance(maxCapacity);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(replicaSubscription.consumer);
            assertThat(assignment.groupAssignment().get(replicaSubscription.consumer).partitions()).hasSize(maxCapacity);
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

            String instance1 = randomString();
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, List.of(replicaPartitions.get(1)));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, List.of(masterPartitions.get(0)));

            String instance2 = randomString();
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, List.of(replicaPartitions.get(0)));
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, List.of(masterPartitions.get(1)));

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instance1ReplicaSubscription, instance1MasterSubscription, instance2ReplicaSubscription, instance2MasterSubscription);

            givenMaxAssignmentsPerInstance(maxCapacity);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(instance1ReplicaSubscription.consumer, instance1MasterSubscription.consumer, instance2ReplicaSubscription.consumer, instance2MasterSubscription.consumer);
            assertThat(assignment.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(0));
            assertThat(assignment.groupAssignment().get(instance2MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(1));
            assertThat(assignment.groupAssignment()).is(
                anyOf(
                    allOf(new AssignmentMapContainsCondition(instance1ReplicaSubscription.consumer, singleton(replicaPartitions.get(2))),
                          new AssignmentMapContainsCondition(instance2ReplicaSubscription.consumer, singleton(replicaPartitions.get(0)))),

                    allOf(new AssignmentMapContainsCondition(instance1ReplicaSubscription.consumer, singleton(replicaPartitions.get(1))),
                          new AssignmentMapContainsCondition(instance2ReplicaSubscription.consumer, singleton(replicaPartitions.get(2))))
                )
            );
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

    @Nested
    class InstanceStateScenarios {

        @Test
        @DisplayName("terminating master, stable replica - unassign master")
        void unassignTerminatingMasterWithStableReplica() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = randomString();
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, emptyList(), app(TERMINATING));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, singletonList(masterPartitions.get(0)), app(TERMINATING));

            String instance2 = randomString();
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, singletonList(replicaPartitions.get(0)), app(STABLE));
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, emptyList(), app(STABLE));

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instance1ReplicaSubscription, instance1MasterSubscription, instance2ReplicaSubscription, instance2MasterSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(instance1ReplicaSubscription.consumer, instance1MasterSubscription.consumer, instance2ReplicaSubscription.consumer, instance2MasterSubscription.consumer);
            assertThat(assignment.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance1ReplicaSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance2MasterSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance2ReplicaSubscription.consumer).partitions()).containsExactlyInAnyOrder(replicaPartitions.get(0));
        }

        @Test
        @DisplayName("terminating master, replicating replica - keep master assigned")
        void keepTerminatingMasterWithReplicatingReplica() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = randomString();
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, emptyList(), app(TERMINATING));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, singletonList(masterPartitions.get(0)), app(TERMINATING));

            String instance2 = randomString();
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, singletonList(replicaPartitions.get(0)), app(REPLICATING));
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, emptyList(), app(REPLICATING));

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instance1ReplicaSubscription, instance1MasterSubscription, instance2ReplicaSubscription, instance2MasterSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(instance1ReplicaSubscription.consumer, instance1MasterSubscription.consumer, instance2ReplicaSubscription.consumer, instance2MasterSubscription.consumer);
            assertThat(assignment.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(0));
            assertThat(assignment.groupAssignment().get(instance1ReplicaSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance2MasterSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance2ReplicaSubscription.consumer).partitions()).containsExactlyInAnyOrder(replicaPartitions.get(0));
        }

        @Test
        @DisplayName("terminating master, no replica - keep master assigned")
        void keepTerminatingMasterWithNoReplica() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 0;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = randomString();
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, emptyList(), app(TERMINATING));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, singletonList(masterPartitions.get(0)), app(TERMINATING));

            String instance2 = randomString();
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, emptyList(), app(STABLE));
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, emptyList(), app(STABLE));

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instance1ReplicaSubscription, instance1MasterSubscription, instance2ReplicaSubscription, instance2MasterSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(instance1ReplicaSubscription.consumer, instance1MasterSubscription.consumer, instance2ReplicaSubscription.consumer, instance2MasterSubscription.consumer);
            assertThat(assignment.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(0));
            assertThat(assignment.groupAssignment().get(instance1ReplicaSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance2MasterSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance2ReplicaSubscription.consumer).partitions()).isEmpty();
        }

        @Test
        @DisplayName("terminating replica - unassign replica")
        void unassignTerminatingReplica() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = randomString();
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, emptyList(), app(STABLE));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, singletonList(masterPartitions.get(0)), app(STABLE));

            String instance2 = randomString();
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, singletonList(replicaPartitions.get(0)), app(TERMINATING));
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, emptyList(), app(TERMINATING));

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instance1ReplicaSubscription, instance1MasterSubscription, instance2ReplicaSubscription, instance2MasterSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(instance1ReplicaSubscription.consumer, instance1MasterSubscription.consumer, instance2ReplicaSubscription.consumer, instance2MasterSubscription.consumer);
            assertThat(assignment.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(0));
            assertThat(assignment.groupAssignment().get(instance1ReplicaSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance2MasterSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance2ReplicaSubscription.consumer).partitions()).isEmpty();
        }


        @Test
        @DisplayName("terminating master, stable replica not on new instance - unassign replica")
        void unassignReplicaOfTerminatingMasterWithStableReplicaNotOnNewInstance() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = randomString();
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, emptyList(), app(TERMINATING));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, singletonList(masterPartitions.get(0)), app(TERMINATING));

            String instance2 = randomString();
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, singletonList(replicaPartitions.get(0)), app(STABLE));
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, emptyList(), app(STABLE));

            String instance3 = randomString();
            ConsumerSubscription instance3ReplicaSubscription = replicaConsumer(instance3, emptyList(), newApp(STABLE));
            ConsumerSubscription instance3MasterSubscription = masterConsumer(instance3, emptyList(), newApp(STABLE));

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instance1ReplicaSubscription, instance1MasterSubscription, instance2ReplicaSubscription, instance2MasterSubscription, instance3ReplicaSubscription, instance3MasterSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(instance1ReplicaSubscription.consumer, instance1MasterSubscription.consumer, instance2ReplicaSubscription.consumer, instance2MasterSubscription.consumer, instance3ReplicaSubscription.consumer, instance3MasterSubscription.consumer);
            assertThat(assignment.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(0));
            assertThat(assignment.groupAssignment().get(instance1ReplicaSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance2MasterSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance2ReplicaSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance3MasterSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance3ReplicaSubscription.consumer).partitions()).isEmpty();
        }

        @Test
        @DisplayName("terminating master, stable replica on new instance - unassign replica")
        void unassignTerminatingMasterWithStableReplicaOnNewInstance() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = "instance1";
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, emptyList(), app(TERMINATING));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, singletonList(masterPartitions.get(0)), app(TERMINATING));

            String instance2 = "instance2";
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, emptyList(), app(STABLE));
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, emptyList(), app(STABLE));

            String instance3 = "instance3";
            ConsumerSubscription instance3ReplicaSubscription = replicaConsumer(instance3, singletonList(replicaPartitions.get(0)), newApp(STABLE));
            ConsumerSubscription instance3MasterSubscription = masterConsumer(instance3, emptyList(), newApp(STABLE));

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instance1ReplicaSubscription, instance1MasterSubscription, instance2ReplicaSubscription, instance2MasterSubscription, instance3ReplicaSubscription, instance3MasterSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(instance1ReplicaSubscription.consumer, instance1MasterSubscription.consumer, instance2ReplicaSubscription.consumer, instance2MasterSubscription.consumer, instance3ReplicaSubscription.consumer, instance3MasterSubscription.consumer);
            assertThat(assignment.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance1ReplicaSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance2MasterSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance2ReplicaSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance3MasterSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance3ReplicaSubscription.consumer).partitions()).containsExactlyInAnyOrder(replicaPartitions.get(0));
        }

        @Test
        @DisplayName("terminating master, no replica and a new instance - keep master assigned")
        void keepTerminatingMasterWithNoReplicaAndNewInstance() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 0;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = "instance1";
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, emptyList(), app(TERMINATING));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, singletonList(masterPartitions.get(0)), app(TERMINATING));

            String instance2 = "instance2";
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, emptyList(), app(STABLE));
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, emptyList(), app(STABLE));

            String instance3 = "instance3";
            ConsumerSubscription instance3ReplicaSubscription = replicaConsumer(instance3, emptyList(), newApp(STABLE));
            ConsumerSubscription instance3MasterSubscription = masterConsumer(instance3, emptyList(), newApp(STABLE));

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instance1ReplicaSubscription, instance1MasterSubscription, instance2ReplicaSubscription, instance2MasterSubscription, instance3ReplicaSubscription, instance3MasterSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(instance1ReplicaSubscription.consumer, instance1MasterSubscription.consumer, instance2ReplicaSubscription.consumer, instance2MasterSubscription.consumer, instance3ReplicaSubscription.consumer, instance3MasterSubscription.consumer);
            assertThat(assignment.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(0));
            assertThat(assignment.groupAssignment().get(instance1ReplicaSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance2MasterSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance2ReplicaSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance3MasterSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance3ReplicaSubscription.consumer).partitions()).isEmpty();
        }

        @Test
        @DisplayName("terminating replica, a new instance - unassign replica")
        void unassignTerminatingReplicaWithNewInstance() {
//        given
            Integer masterPartitionsCount = 1;
            Integer replicaPartitionsCount = 1;
            List<TopicPartition> masterPartitions = masterPartitions(masterPartitionsCount);
            List<TopicPartition> replicaPartitions = replicaPartitions(replicaPartitionsCount);

            String instance1 = "instance1";
            ConsumerSubscription instance1ReplicaSubscription = replicaConsumer(instance1, emptyList(), app(STABLE));
            ConsumerSubscription instance1MasterSubscription = masterConsumer(instance1, singletonList(masterPartitions.get(0)), app(STABLE));

            String instance2 = "instance2";
            ConsumerSubscription instance2ReplicaSubscription = replicaConsumer(instance2, singletonList(replicaPartitions.get(0)), app(TERMINATING));
            ConsumerSubscription instance2MasterSubscription = masterConsumer(instance2, emptyList(), app(TERMINATING));

            String instance3 = "instance3";
            ConsumerSubscription instance3ReplicaSubscription = replicaConsumer(instance3, emptyList(), newApp(STABLE));
            ConsumerSubscription instance3MasterSubscription = masterConsumer(instance3, emptyList(), newApp(STABLE));

            Cluster cluster = cluster(masterPartitionsCount, replicaPartitionsCount);
            ConsumerPartitionAssignor.GroupSubscription groupSubscription = groupSubscription(instance1ReplicaSubscription, instance1MasterSubscription, instance2ReplicaSubscription, instance2MasterSubscription, instance3ReplicaSubscription, instance3MasterSubscription);

//        when
            ConsumerPartitionAssignor.GroupAssignment assignment = assignor.assign(cluster, groupSubscription);

//        then
            assertNotNull(assignment);
            assertNotNull(assignment.groupAssignment());
            assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrder(instance1ReplicaSubscription.consumer, instance1MasterSubscription.consumer, instance2ReplicaSubscription.consumer, instance2MasterSubscription.consumer, instance3ReplicaSubscription.consumer, instance3MasterSubscription.consumer);
            assertThat(assignment.groupAssignment().get(instance1MasterSubscription.consumer).partitions()).containsExactlyInAnyOrder(masterPartitions.get(0));
            assertThat(assignment.groupAssignment().get(instance1ReplicaSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance2MasterSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance2ReplicaSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance3MasterSubscription.consumer).partitions()).isEmpty();
            assertThat(assignment.groupAssignment().get(instance3ReplicaSubscription.consumer).partitions()).isEmpty();
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
        return consumer(instance, MASTER_TOPIC, ownedPartitions, app(STABLE));
    }

    ConsumerSubscription replicaConsumer(String instance, List<TopicPartition> ownedPartitions) {
        return consumer(instance, REPLICA_TOPIC, ownedPartitions, app(STABLE));
    }

    ConsumerSubscription masterConsumer(String instance, List<TopicPartition> ownedPartitions, ApplicationStateContext.ApplicationDetails applicationDetails) {
        return consumer(instance, MASTER_TOPIC, ownedPartitions, applicationDetails);
    }

    ConsumerSubscription replicaConsumer(String instance, List<TopicPartition> ownedPartitions, ApplicationStateContext.ApplicationDetails applicationDetails) {
        return consumer(instance, REPLICA_TOPIC, ownedPartitions, applicationDetails);
    }

    ApplicationStateContext.ApplicationDetails app(ApplicationStateContext.ApplicationState state) {
        return new ApplicationStateContext.ApplicationDetails(state, false);
    }

    ApplicationStateContext.ApplicationDetails newApp(ApplicationStateContext.ApplicationState state) {
        return new ApplicationStateContext.ApplicationDetails(state, true);
    }

    ConsumerSubscription consumer(String instance, String topic, List<TopicPartition> ownedPartitions, ApplicationStateContext.ApplicationDetails applicationDetails) {
        List<String> topics = singletonList(topic);
        AssignmentMetadata assignmentMetadata = new AssignmentMetadata(instance, applicationDetails);
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