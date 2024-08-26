package com.pg.replication.consumer.kafka.assignment.v1;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.commons.lang3.stream.IntStreams;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReplicationCooperativeAssignor implements ConsumerPartitionAssignor, Configurable {

    private static final ObjectMapper mapper = new ObjectMapper();

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
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
//        todo
        ConsumerPartitionAssignor.super.onAssignment(assignment, metadata);
    }

    @Override
    public GroupAssignment assign(Cluster cluster, GroupSubscription groupSubscription) {
        AssignmentContainer assignmentContainer = buildAssignmentContainer(cluster, groupSubscription);
        Map<String, Assignment> assignment = assignmentContainer.assign();
        return new GroupAssignment(assignment);
    }

    private AssignmentContainer buildAssignmentContainer(Cluster cluster, GroupSubscription groupSubscription) {
        AssignmentContainer assignmentContainer = new AssignmentContainer(config.getMasterTopic(), config.getReplicaTopic(), config.getMaxAssignmentsPerInstance());
        Set<String> seenTopics = new HashSet<>(cluster.topics().size());
        for (Map.Entry<String, Subscription> consumerSubscription : groupSubscription.groupSubscription().entrySet()) {
            String consumer = consumerSubscription.getKey();
            Subscription subscription = consumerSubscription.getValue();
            String instance = decodeAssignmentMetadata(subscription.userData()).getInstance();
            List<String> topics = subscription.topics();
            assignmentContainer.addInstanceConsumer(instance, consumer, new HashSet<>(topics));

            for (String topic : topics) {
                if (seenTopics.contains(topic)) {
                    continue;
                }

                Integer partitionsForTopic = cluster.partitionCountForTopic(topic);
                IntStreams.range(partitionsForTopic).mapToObj(i -> new TopicPartition(topic, i)).forEach(assignmentContainer::addPartition);
                seenTopics.add(topic);
            }

            for (TopicPartition topicPartition : subscription.ownedPartitions()) {
                assignmentContainer.addAssignment(topicPartition, instance, consumer);
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
        this.assignmentMetadata = new AssignmentMetadata(this.config.getInstanceId());
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
