package com.pg.replication.consumer.kafka.assignor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pg.replication.consumer.lifecycle.ApplicationStateContext;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ReplicationCooperativeAssignor implements ConsumerPartitionAssignor, Configurable {

    private static final ObjectMapper mapper = new ObjectMapper();

    ReplicationCooperativeAssignorConfig config;
    String instanceId;

    @Override
    public List<RebalanceProtocol> supportedProtocols() {
        return List.of(RebalanceProtocol.COOPERATIVE);
    }

    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        AssignmentMetadata metadata = AssignmentMetadata.builder()
                .instance(instanceId)
                .state(ApplicationStateContext.getState())
                .build();

        return encodeAssignmentMetadata(metadata);
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

        for (Map.Entry<String, Subscription> consumerSubscription : groupSubscription.groupSubscription().entrySet()) {
            String consumer = consumerSubscription.getKey();
            Subscription subscription = consumerSubscription.getValue();
            AssignmentMetadata assignmentMetadata = decodeAssignmentMetadata(subscription.userData());
            String instance = assignmentMetadata.getInstance();
            ApplicationStateContext.ApplicationState instanceState = assignmentMetadata.state;
            List<String> topics = subscription.topics();
            assignmentContainer.addInstanceConsumer(instance, instanceState, consumer, topics);

            for (TopicPartition topicPartition : subscription.ownedPartitions()) {
                assignmentContainer.addAssignment(topicPartition, instance);
            }
        }
        return assignmentContainer;
    }

    @Override
    public String name() {
        return "replica-cooperative-v1";
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.config = new ReplicationCooperativeAssignorConfig(configs);
        this.instanceId = this.config.getInstanceId();
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
}
