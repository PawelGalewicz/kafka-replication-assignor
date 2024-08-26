package com.pg.replication.consumer.v2;

import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@NoArgsConstructor
@Setter
public class AssignmentMetadata implements Serializable {
    public Map<String, Map<Integer, ReplicationType>> topicToPartitionAssignment = new HashMap<>();

    public Optional<ReplicationType> getReplicationType(String topic, Integer partition) {
        return Optional.ofNullable(topicToPartitionAssignment.get(topic))
                .flatMap(m -> Optional.ofNullable(m.get(partition)));
    }

    public void add(TopicPartition topicPartition, ReplicationType replicationType) {
        topicToPartitionAssignment.computeIfAbsent(topicPartition.topic(), ignore -> new HashMap<>())
                .put(topicPartition.partition(), replicationType);
    }
}
