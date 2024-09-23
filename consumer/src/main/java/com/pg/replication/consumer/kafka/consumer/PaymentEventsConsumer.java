package com.pg.replication.consumer.kafka.consumer;

import com.pg.replication.common.event.*;
import com.pg.replication.consumer.partition.PartitionAssignmentService;
import com.pg.replication.consumer.payment.PaymentEventHandler;
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
public class PaymentEventsConsumer implements ConsumerSeekAware {

    private final PaymentEventHandler paymentEventHandler;
    private final PartitionAssignmentService partitionAssignmentService;

    @KafkaListener(topics = "${kafka.topic.master}", groupId = "${kafka.group-id}", containerFactory = "kafkaListenerContainerFactory")
    public void listenToPaymentEvents(@Payload PaymentEvent event, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        log.debug("Received message {} from master partition: {}", event, partition);
        handlePaymentEvent(event, partition);
    }

    void handlePaymentEvent(PaymentEvent paymentEvent, Integer sourcePartition) {
        switch (paymentEvent) {
            case PaymentCreatedEvent e -> paymentEventHandler.handlePaymentCreatedEvent(e, sourcePartition);
            case PaymentAuthorisedEvent e -> paymentEventHandler.handlePaymentAuthorisedEvent(e);
            case PaymentClearedEvent e -> paymentEventHandler.handlePaymentClearedEvent(e);
            case PaymentAmountChangedEvent e -> paymentEventHandler.handlePaymentAmountChangedEvent(e);
            case PaymentFinishedEvent e -> paymentEventHandler.handlePaymentFinishedEvent(e);
            case null -> log.warn("unexpected null event received");
            default -> log.warn("unexpected payment event received: {}", paymentEvent);
        }
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, @NonNull ConsumerSeekCallback callback) {
        if (assignments.isEmpty()) {
            return;
        }

        Set<Integer> assignedPartitions = assignments.keySet()
                .stream()
                .map(TopicPartition::partition)
                .collect(Collectors.toSet());

        partitionAssignmentService.addAssignedMasterPartitions(assignedPartitions);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return;
        }

        Set<Integer> revokedPartitions = partitions
                .stream()
                .map(TopicPartition::partition)
                .collect(Collectors.toSet());

        partitionAssignmentService.revokeMasterPartitions(revokedPartitions);
    }
}
