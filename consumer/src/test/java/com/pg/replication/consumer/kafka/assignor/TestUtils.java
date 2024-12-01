package com.pg.replication.consumer.kafka.assignor;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.stream.IntStreams;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Condition;
import org.assertj.core.api.ListAssert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TestUtils {
    public static final String MASTER_TOPIC = "master_topic";
    public static final String REPLICA_TOPIC = "replica_topic";

    public static ListAssert<TopicPartition> assertConsumerAssignment(ConsumerPartitionAssignor.GroupAssignment assignment, String consumer) {
        return assertThat(assignment.groupAssignment().get(consumer).partitions());
    }

    public static void assertAssignmentContainsAllInstances(ConsumerPartitionAssignor.GroupAssignment assignment, Instance... instances) {
        Set<String> consumers = Arrays.stream(instances).flatMap(i -> Stream.of(i.getMasterConsumer(), i.getReplicaConsumer())).collect(Collectors.toSet());
        assertNotNull(assignment);
        assertNotNull(assignment.groupAssignment());
        assertThat(assignment.groupAssignment().keySet()).containsExactlyInAnyOrderElementsOf(consumers);
    }

    public static String randomString() {
        return RandomStringUtils.randomAlphabetic(10);
    }

    public static ConsumerPartitionAssignor.GroupSubscription groupSubscription(Instance... instances) {
        Map<String, ConsumerPartitionAssignor.Subscription> subscriptionMap = new HashMap<>();
        for (Instance instance : instances) {
            subscriptionMap.put(instance.getMasterConsumer(), instance.getMasterSubscription());
            subscriptionMap.put(instance.getReplicaConsumer(), instance.getReplicaSubscription());
        }
        return new ConsumerPartitionAssignor.GroupSubscription(subscriptionMap);
    }

    public static List<TopicPartition> masterPartitions(Integer partitions) {
        return topicPartitions(MASTER_TOPIC, partitions);
    }

    public static List<TopicPartition> replicaPartitions(Integer partitions) {
        return topicPartitions(REPLICA_TOPIC, partitions);
    }

    public static List<TopicPartition> topicPartitions(String topic, Integer partitions) {
        return IntStreams.range(partitions)
                .mapToObj(p -> new TopicPartition(topic, p))
                .collect(Collectors.toList());
    }

    public static Cluster cluster(Integer masterPartitions, Integer replicaPartitions) {
        String clusterId = randomString();
        Set<PartitionInfo> partitions = new HashSet<>(masterPartitions + replicaPartitions);

        IntStreams.range(masterPartitions)
                .forEach(p -> partitions.add(new PartitionInfo(MASTER_TOPIC, p, null, null, null)));

        IntStreams.range(replicaPartitions)
                .forEach(p -> partitions.add(new PartitionInfo(REPLICA_TOPIC, p, null, null, null)));

        return new Cluster(clusterId, emptySet(), partitions, emptySet(), emptySet());
    }

    public static class AssignmentMapContainsCondition extends Condition<Map<String, ConsumerPartitionAssignor.Assignment>> {

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

    public static class AssignmentMapCountCondition extends Condition<Map<String, ConsumerPartitionAssignor.Assignment>> {

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

    public static class AssignmentMapEmptyCondition extends Condition<Map<String, ConsumerPartitionAssignor.Assignment>> {

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
