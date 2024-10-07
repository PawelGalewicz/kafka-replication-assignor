package com.pg.replication.consumer.kafka.consumer;

import com.pg.replication.common.event.PaymentReplicaDeletedEvent;
import com.pg.replication.common.event.PaymentReplicaEvent;
import com.pg.replication.common.event.PaymentReplicaUpdatedEvent;
import com.pg.replication.common.event.PaymentReplicationStartedEvent;
import com.pg.replication.consumer.assignment.PartitionAssignmentService;
import com.pg.replication.consumer.payment.PaymentEventHandler;
import com.pg.replication.consumer.replication.ReplicationProcessService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.lang.NonNull;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Slf4j
@AllArgsConstructor
public class PaymentReplicationEventsConsumer implements ConsumerSeekAware {

    public static final String REPLICA_LISTENER_ID = "replica-listener";

    private final PaymentEventHandler paymentEventHandler;
    private final ReplicationProcessService replicationProcessService;
    private final PartitionAssignmentService partitionAssignmentService;

    @KafkaListener(topics = "${kafka.topic.replica}", id = REPLICA_LISTENER_ID, groupId = "${kafka.group-id}", containerFactory = "kafkaListenerContainerFactory")
    public void listenToPaymentReplicaEvents(@Payload(required = false) PaymentReplicaEvent event, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        log.debug("Received message: {} from replica partition: {}", event, partition);
        handlePaymentEvent(event);
    }

    void handlePaymentEvent(PaymentReplicaEvent paymentEvent) {
        switch (paymentEvent) {
            case PaymentReplicaUpdatedEvent e -> paymentEventHandler.handlePaymentReplicaUpdatedEvent(e);
            case PaymentReplicaDeletedEvent e -> paymentEventHandler.handlePaymentReplicaDeletedEvent(e);
            case PaymentReplicationStartedEvent e -> paymentEventHandler.handlePaymentReplicationStartedEvent(e);
            case null -> log.warn("null event received, might be a delete of replication event");
            default -> log.warn("unexpected payment event received: {}", paymentEvent);
        }
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, @NonNull ConsumerSeekCallback callback) {
//        it is important not to call seekToBeginning even with empty set as it seems that it will seek to beginning
//        with all of previously assigned partitions
        if (assignments.isEmpty()) {
            return;
        }

        Set<TopicPartition> topicPartitions = assignments.keySet();

        Set<Integer> replicaPartitions = topicPartitions.stream()
                .map(TopicPartition::partition)
                .collect(Collectors.toSet());

        replicaPartitions.forEach(replicationProcessService::startReplicationProcess);
        partitionAssignmentService.addAssignedReplicaPartitions(replicaPartitions);

        callback.seekToBeginning(topicPartitions);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return;
        }

        Set<Integer> revokedPartitions = partitions.stream()
                .map(TopicPartition::partition)
                .collect(Collectors.toSet());

        partitionAssignmentService.revokeReplicaPartitions(revokedPartitions);
    }
}
