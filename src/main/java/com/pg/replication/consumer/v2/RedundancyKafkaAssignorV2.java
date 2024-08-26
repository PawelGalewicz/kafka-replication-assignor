package com.pg.replication.consumer.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.commons.lang3.stream.IntStreams;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class RedundancyKafkaAssignorV2 implements ConsumerPartitionAssignor {

    public static final Integer REPLICATION_FACTOR = 2;

    private final ObjectMapper mapper = new ObjectMapper();

    static AssignmentMetadata assignmentMetadata = new AssignmentMetadata();

    @Override
    public String name() {
        return "redundancy";
    }

    @Override
    public List<RebalanceProtocol> supportedProtocols() {
        return Collections.singletonList(RebalanceProtocol.COOPERATIVE);
    }

    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        return encodeAssignmentMetadata(assignmentMetadata);
    }

    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        decodeAssignmentMetadata(assignment.userData()).ifPresent(a -> assignmentMetadata = a);
    }

    @Override
    public GroupAssignment assign(Cluster cluster, GroupSubscription groupSubscription) {
        Map<String, TopicAssignmentContainer> topicToAssignmentContainers = buildTopicAssignmentContainers(cluster, groupSubscription.groupSubscription());

        for (TopicAssignmentContainer topicAssignmentContainer : topicToAssignmentContainers.values()) {
            assign(groupSubscription.groupSubscription().keySet(), topicAssignmentContainer);
        }

        Map<String, Assignment> assignmentMap = topicToAssignmentContainers.values().stream()
                .flatMap(TopicAssignmentContainer::getConsumerPartitionAssignmentsStream)
                .collect(Collectors.groupingBy(Map.Entry::getKey,
                            Collectors.mapping(Map.Entry::getValue,
                                Collectors.collectingAndThen(Collectors.toList(), this::toAssignment))));


        return new GroupAssignment(assignmentMap);
    }

    private Map<String, TopicAssignmentContainer> buildTopicAssignmentContainers(Cluster cluster, Map<String, Subscription> consumerToSubscription) {
        Map<String, TopicAssignmentContainer> topicToPartitionContainers = new HashMap<>();
        for (Map.Entry<String, Subscription> consumerSubscriptionEntry : consumerToSubscription.entrySet()) {
            String consumer = consumerSubscriptionEntry.getKey();
            Subscription subscription = consumerSubscriptionEntry.getValue();

            for (String topic : subscription.topics()) {
                if (topicToPartitionContainers.containsKey(topic)) {
                    continue;
                }

                Integer topicPartitions = cluster.partitionCountForTopic(topic);
                if (Objects.nonNull(topicPartitions)) {
                    topicToPartitionContainers.put(topic, new TopicAssignmentContainer(topic, topicPartitions, REPLICATION_FACTOR));
                }
            }

            decodeAssignmentMetadata(subscription.userData()).ifPresent(consumerAssignmentMetadata -> {
                for (TopicPartition topicPartition : subscription.ownedPartitions()) {
                    String topic = topicPartition.topic();
                    int partition = topicPartition.partition();

                    Optional<ReplicationType> replicationType = consumerAssignmentMetadata.getReplicationType(topic, partition);
                    if (replicationType.isEmpty()) {
                        continue;
                    }

                    switch (replicationType.get()) {
                        case REPLICA -> topicToPartitionContainers.get(topic).addPartitionReplica(partition, consumer);
                        case MASTER -> topicToPartitionContainers.get(topic).addPartitionMaster(partition, consumer);
                    }
                }
            });
        }
        return topicToPartitionContainers;
    }

    private void assign(Set<String> consumers, TopicAssignmentContainer topicAssignmentContainer) {
        MissingAssignments missingAssignments = getMissingPartitionAssignments(topicAssignmentContainer.getPartitionAssignments());
        for (PartitionAssignment masterAssignment : missingAssignments.masterAssignments) {
            int partition = masterAssignment.topicPartition().partition();
            Collection<String> masterCandidatesFromReplicas = topicAssignmentContainer.getReplicaConsumers(partition);

            if (!masterCandidatesFromReplicas.isEmpty()) {
                masterCandidatesFromReplicas.stream()
                        .min(Comparator
                                .comparing(topicAssignmentContainer::getMasterAssignments)
                                .thenComparing(topicAssignmentContainer::getPartitionAssignments))
                        .ifPresent(consumer -> {
                            topicAssignmentContainer.promoteReplicaToMaster(partition, consumer);
                            missingAssignments.addReplica(masterAssignment.topicPartition());
                        });
            } else {
                consumers.stream()
                        .min(Comparator
                                .comparing(topicAssignmentContainer::getMasterAssignments)
                                .thenComparing(topicAssignmentContainer::getPartitionAssignments))
                        .ifPresent(consumer -> topicAssignmentContainer.addPartitionMaster(partition, consumer));
            }
        }

        for (PartitionAssignment masterAssignment : missingAssignments.replicaAssignments) {
            int partition = masterAssignment.topicPartition().partition();
            consumers.stream()
                    .min(Comparator.comparing(topicAssignmentContainer::getPartitionAssignments))
                    .ifPresent(consumer -> topicAssignmentContainer.addPartitionReplica(partition, consumer));
        }
    }

    private MissingAssignments getMissingPartitionAssignments(Collection<PartitionAssignmentContainer> previousAssignments) {
        MissingAssignments missingAssignments = new MissingAssignments();
        previousAssignments.forEach(c -> {
            if (!c.hasMaster()) {
                missingAssignments.addMaster(c.getTopicPartition());
            }

            IntStreams.range(c.countMissingReplicas())
                    .forEach(ignore -> missingAssignments.addReplica(c.getTopicPartition()));
        });
        return missingAssignments;
    }

    private Assignment toAssignment(List<PartitionAssignment> partitionAssignments) {
        List<TopicPartition> topicPartitions = new ArrayList<>(partitionAssignments.size());
        AssignmentMetadata assignmentMetadata = new AssignmentMetadata();
        for (PartitionAssignment partitionAssignment : partitionAssignments) {
            topicPartitions.add(partitionAssignment.topicPartition());
            assignmentMetadata.add(partitionAssignment.topicPartition(), partitionAssignment.replicationType());
        }
        return new Assignment(topicPartitions, encodeAssignmentMetadata(assignmentMetadata));
    }

    @SneakyThrows
    public ByteBuffer encodeAssignmentMetadata(AssignmentMetadata assignmentMetadata) {
        byte[] bytes = mapper.writeValueAsString(assignmentMetadata).getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.wrap(bytes);
    }

    @SneakyThrows
    private Optional<AssignmentMetadata> decodeAssignmentMetadata(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return Optional.empty();
        }
        String userDataString = StandardCharsets.UTF_8.decode(byteBuffer).toString();
        return Optional.of(mapper.readValue(userDataString, AssignmentMetadata.class));
    }

    static class MissingAssignments {
        List<PartitionAssignment> masterAssignments = new ArrayList<>();
        List<PartitionAssignment> replicaAssignments = new ArrayList<>();

        public void addMaster(TopicPartition topicPartition) {
            masterAssignments.add(new PartitionAssignment(topicPartition, ReplicationType.MASTER));
        }

        public void addReplica(TopicPartition topicPartition) {
            replicaAssignments.add(new PartitionAssignment(topicPartition, ReplicationType.REPLICA));
        }
    }
}
