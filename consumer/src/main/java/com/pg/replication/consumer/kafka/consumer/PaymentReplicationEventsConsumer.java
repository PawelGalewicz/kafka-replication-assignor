package com.pg.replication.consumer.kafka.consumer;

import com.pg.replication.common.event.PaymentReplicaDeletedEvent;
import com.pg.replication.common.event.PaymentReplicaEvent;
import com.pg.replication.common.event.PaymentReplicaUpdatedEvent;
import com.pg.replication.common.event.PaymentReplicationStartedEvent;
import com.pg.replication.consumer.partition.PartitionAssignmentService;
import com.pg.replication.consumer.payment.PaymentEventHandler;
import com.pg.replication.consumer.replication.ReplicationProcessService;
import lombok.AllArgsConstructor;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@AllArgsConstructor
public class PaymentReplicationEventsConsumer implements ConsumerSeekAware {

    private final PaymentEventHandler paymentEventHandler;
    private final ReplicationProcessService replicationProcessService;
    private final PartitionAssignmentService partitionAssignmentService;

    @KafkaListener(topics = "${kafka.topic.replica}", containerFactory = "kafkaListenerContainerFactory")
    public void listenToPaymentReplicaEvents(@Payload(required = false) PaymentReplicaEvent event, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        System.out.println("Received message: " + event + " from replica partition: " + partition);
        handlePaymentEvent(event);
    }

    void handlePaymentEvent(PaymentReplicaEvent paymentEvent) {
        switch (paymentEvent) {
            case PaymentReplicaUpdatedEvent e -> paymentEventHandler.handlePaymentReplicaUpdatedEvent(e);
            case PaymentReplicaDeletedEvent e -> paymentEventHandler.handlePaymentReplicaDeletedEvent(e);
            case PaymentReplicationStartedEvent e -> paymentEventHandler.handlePaymentReplicationStartedEvent(e);
            case null, default -> System.out.println("null event");
        }
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        Set<TopicPartition> topicPartitions = assignments.keySet();
        System.out.println("New replica partitions assigned: " + topicPartitions);

        Set<Integer> replicaPartitions = topicPartitions.stream()
                .map(TopicPartition::partition)
                .collect(Collectors.toSet());

        replicaPartitions.forEach(replicationProcessService::startReplicationProcess);
        partitionAssignmentService.addAssignedReplicaPartitions(replicaPartitions);

        callback.seekToBeginning(topicPartitions);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("Replica partitions revoked: " + partitions.toString());

        Set<Integer> revokedPartitions = partitions.stream()
                .map(TopicPartition::partition)
                .collect(Collectors.toSet());

        partitionAssignmentService.revokeReplicaPartitions(revokedPartitions);
    }
}
