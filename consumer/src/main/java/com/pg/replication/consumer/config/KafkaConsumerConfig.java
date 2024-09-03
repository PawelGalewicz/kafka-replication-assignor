package com.pg.replication.consumer.config;

import com.pg.replication.common.event.PaymentEvent;
import com.pg.replication.consumer.kafka.assignment.v1.ReplicationCooperativeAssignor;
import com.pg.replication.consumer.kafka.assignment.v1.ReplicationCooperativeAssignorConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${kafka.topic.master}")
    private String inputTopicName;

    @Value(value = "${kafka.topic.replica}")
    private String replicationTopicName;

    @Value(value = "${application.instance.id}")
    private String instanceId;

    @Value(value = "${application.instance.max_assignments}")
    private String maxInstanceAssignment;

    public ConsumerFactory<String, PaymentEvent> consumerFactory(String groupId) {
        System.out.println("Instance id: " + instanceId);
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "20971520");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "20971520");
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, ReplicationCooperativeAssignor.class.getName());
        props.put(ReplicationCooperativeAssignorConfig.MASTER_TOPIC, inputTopicName);
        props.put(ReplicationCooperativeAssignorConfig.REPLICA_TOPIC, replicationTopicName);
        props.put(ReplicationCooperativeAssignorConfig.INSTANCE_ID, instanceId);
        props.put(ReplicationCooperativeAssignorConfig.MAX_ASSIGNMENTS_PER_INSTANCE, maxInstanceAssignment);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), jsonDeserializer());
    }

    public JsonDeserializer<PaymentEvent> jsonDeserializer() {
        JsonDeserializer<PaymentEvent> objectJsonDeserializer = new JsonDeserializer<>();
        objectJsonDeserializer.addTrustedPackages("*", "com.pg.replication.common.event");
        return objectJsonDeserializer;
    }

    public ConcurrentKafkaListenerContainerFactory<String, PaymentEvent> kafkaListenerContainerFactory(String groupId) {
        ConcurrentKafkaListenerContainerFactory<String, PaymentEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(groupId));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, PaymentEvent> factory = kafkaListenerContainerFactory("consumer");
//        fixme custom rebalance listener, maybe it could help
//        factory.getContainerProperties().setConsumerRebalanceListener();
        return factory;
    }
}
