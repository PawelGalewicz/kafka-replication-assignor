package com.pg.replication.consumer.v2;

import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;

public record PartitionAssignment(TopicPartition topicPartition,
                                  ReplicationType replicationType)
        implements Serializable {
}
