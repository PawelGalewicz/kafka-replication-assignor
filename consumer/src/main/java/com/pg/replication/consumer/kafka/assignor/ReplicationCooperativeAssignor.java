package com.pg.replication.consumer.kafka.assignor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

public class ReplicationCooperativeAssignor implements ConsumerPartitionAssignor, Configurable {

    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    ReplicationCooperativeAssignorConfig config;
    AssignmentMetadata assignmentMetadata;

    @Override
    public List<RebalanceProtocol> supportedProtocols() {
        return List.of(RebalanceProtocol.COOPERATIVE);
    }

    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        return encodeAssignmentMetadata(assignmentMetadata);
    }

    @Override
    public GroupAssignment assign(Cluster cluster, GroupSubscription groupSubscription) {
        AssignmentContainer assignmentContainer = buildAssignmentContainer(cluster, groupSubscription);
        Map<String, Assignment> assignment = assignmentContainer.assign();
        return new GroupAssignment(assignment);
    }

    private AssignmentContainer buildAssignmentContainer(Cluster cluster, GroupSubscription groupSubscription) {
        Integer masterTopicPartitionCount = Optional.ofNullable(cluster.partitionCountForTopic(config.getMasterTopic()))
                .orElse(0);
        Integer replicaTopicPartitionCount = Optional.ofNullable(cluster.partitionCountForTopic(config.getReplicaTopic()))
                .orElse(0);

        AssignmentContainer assignmentContainer = new AssignmentContainer(
                config.getMasterTopic(),
                config.getReplicaTopic(),
                config.getMaxAssignmentsPerInstance(), masterTopicPartitionCount, replicaTopicPartitionCount);

        List<SubscriptionWithMetadata> subscriptionList = groupSubscription.groupSubscription().entrySet()
                .stream()
                .map(e -> buildSubscriptionWithMetadata(e.getKey(), e.getValue()))
                .sorted(SubscriptionWithMetadata.createdAtComparator())
                .toList();

        for (SubscriptionWithMetadata subscriptionWithMetadata : subscriptionList) {
            Subscription subscription = subscriptionWithMetadata.subscription;
            String instance = subscriptionWithMetadata.assignmentMetadata.instance;
            String consumer = subscriptionWithMetadata.consumer;
            List<String> topics = subscription.topics();
            assignmentContainer.addInstanceConsumer(instance, consumer, new HashSet<>(topics));

            for (TopicPartition topicPartition : subscription.ownedPartitions()) {
                assignmentContainer.addAssignment(topicPartition, instance);
            }
        }

        return assignmentContainer;
    }

    private static SubscriptionWithMetadata buildSubscriptionWithMetadata(String consumer, Subscription subscription) {
        AssignmentMetadata assignmentMetadata = decodeAssignmentMetadata(subscription.userData());
        return new SubscriptionWithMetadata(consumer, subscription, assignmentMetadata);
    }

    @Override
    public String name() {
        return "replica-cooperative-v1";
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.config = new ReplicationCooperativeAssignorConfig(configs);
        this.assignmentMetadata = new AssignmentMetadata(this.config.getInstanceId(), Instant.now());
    }

    @SneakyThrows
    protected static ByteBuffer encodeAssignmentMetadata(AssignmentMetadata assignmentMetadata) {
        byte[] bytes = mapper.writeValueAsString(assignmentMetadata).getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.wrap(bytes);
    }

    @SneakyThrows
    protected static AssignmentMetadata decodeAssignmentMetadata(ByteBuffer byteBuffer) {
        String userDataString = StandardCharsets.UTF_8.decode(byteBuffer).toString();
        return mapper.readValue(userDataString, AssignmentMetadata.class);
    }


    @AllArgsConstructor
    private static final class SubscriptionWithMetadata {
        String consumer;
        Subscription subscription;
        AssignmentMetadata assignmentMetadata;

        static Comparator<SubscriptionWithMetadata> createdAtComparator() {
            return Comparator.comparing(s -> s.assignmentMetadata.instanceCreatedAt);
        }

    }
}
