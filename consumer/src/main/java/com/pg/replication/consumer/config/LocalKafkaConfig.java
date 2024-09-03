package com.pg.replication.consumer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.Map;

@Configuration
@Profile(value = "local")
public class LocalKafkaConfig {

    @Value(value = "${kafka.topic.master}")
    private String masterTopicName;

    @Value(value = "${kafka.topic.replica}")
    private String replicaTopicName;

    @Bean
    public NewTopic masterTopic() {
        return new NewTopic(masterTopicName, 1, (short) 1);
    }

    @Bean
    public NewTopic replicaTopic() {
        NewTopic newTopic = new NewTopic(replicaTopicName, 1, (short) 1);
        newTopic.configs(Map.of(
                "min.cleanable.dirty.ratio", "0.001",
                "cleanup.policy", "compact",
                "segment.ms", "1000",
                "retention.ms", "1000",
                "delete.retention.ms", "100"
        ));
        return newTopic;
    }
}
