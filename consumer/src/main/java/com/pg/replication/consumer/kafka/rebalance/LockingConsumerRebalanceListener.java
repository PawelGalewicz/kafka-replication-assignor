package com.pg.replication.consumer.kafka.rebalance;

import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Component
public class LockingConsumerRebalanceListener implements ConsumerAwareRebalanceListener {

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
//        fixme if topic is replica, make sure you're up to date before you give it away ?
        ConsumerAwareRebalanceListener.super.onPartitionsLost(partitions);
    }
}
