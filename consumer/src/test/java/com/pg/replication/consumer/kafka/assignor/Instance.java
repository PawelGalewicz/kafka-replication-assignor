package com.pg.replication.consumer.kafka.assignor;

import com.pg.replication.consumer.lifecycle.ApplicationStateContext;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static com.pg.replication.consumer.kafka.assignor.TestUtils.MASTER_TOPIC;
import static com.pg.replication.consumer.kafka.assignor.TestUtils.REPLICA_TOPIC;
import static com.pg.replication.consumer.lifecycle.ApplicationStateContext.ApplicationState.STABLE;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class Instance {
    private String name;
    private ConsumerSubscription masterConsumer;
    private ConsumerSubscription replicaConsumer;

    private Instance(String name, ConsumerSubscription masterConsumer, ConsumerSubscription replicaConsumer) {
        this.name = name;
        this.masterConsumer = masterConsumer;
        this.replicaConsumer = replicaConsumer;
    }

    public static InstanceBuilder builder() {
        return new InstanceBuilder();
    }

    public static Instance emptyInstance() {
        return builder().noReplicaPartitions().noMasterPartitions().build();
    }

    public ConsumerPartitionAssignor.Subscription getMasterSubscription() {
        return masterConsumer.subscription;
    }

    public ConsumerPartitionAssignor.Subscription getReplicaSubscription() {
        return replicaConsumer.subscription;
    }

    public String getMasterConsumer() {
        return masterConsumer.consumer;
    }

    public String getReplicaConsumer() {
        return replicaConsumer.consumer;
    }


    public static class InstanceBuilder {
        private String name = RandomStringUtils.randomAlphabetic(10);
        private ApplicationStateContext.ApplicationState state = STABLE;
        private boolean isNew = false;
        private List<TopicPartition> masterPartitions;
        private List<TopicPartition> replicaPartitions;

        public InstanceBuilder name(String name) {
            this.name = name;
            return this;
        }

        public InstanceBuilder isNew() {
            this.isNew = true;
            return this;
        }

        public InstanceBuilder state(ApplicationStateContext.ApplicationState state) {
            this.state = state;
            return this;
        }

        public InstanceBuilder masterPartitions(Integer... integers) {
            this.masterPartitions = Arrays.stream(integers).map(p -> new TopicPartition(MASTER_TOPIC, p)).toList();
            return this;
        }

        public InstanceBuilder replicaPartitions(Integer... integers) {
            this.replicaPartitions = Arrays.stream(integers).map(p -> new TopicPartition(REPLICA_TOPIC, p)).toList();
            return this;
        }

        public InstanceBuilder masterPartitions(List<TopicPartition> masterPartitions) {
            this.masterPartitions = masterPartitions;
            return this;
        }

        public InstanceBuilder replicaPartitions(List<TopicPartition> replicaPartitions) {
            this.replicaPartitions = replicaPartitions;
            return this;
        }

        public InstanceBuilder noMasterPartitions() {
            this.masterPartitions = emptyList();
            return this;
        }

        public InstanceBuilder noReplicaPartitions() {
            this.replicaPartitions = emptyList();
            return this;
        }

        public Instance build() {
            ApplicationStateContext.ApplicationDetails applicationDetails = new ApplicationStateContext.ApplicationDetails(state, isNew);
            return new Instance(name, consumer(name, MASTER_TOPIC, masterPartitions), consumer(name, REPLICA_TOPIC, replicaPartitions));
        }

        private ConsumerSubscription consumer(String instance, String topic, List<TopicPartition> ownedPartitions) {
            List<String> topics = singletonList(topic);
            AssignmentMetadata assignmentMetadata = new AssignmentMetadata(instance, new ApplicationStateContext.ApplicationDetails(state, isNew));
            ByteBuffer userData = ReplicationCooperativeAssignor.encodeAssignmentMetadata(assignmentMetadata);
            ConsumerPartitionAssignor.Subscription subscription = new ConsumerPartitionAssignor.Subscription(topics, userData, ownedPartitions);
            return new ConsumerSubscription(instance + "_" + topic + "_consumer", subscription);
        }
    }

    public record ConsumerSubscription(String consumer, ConsumerPartitionAssignor.Subscription subscription) {}
}
