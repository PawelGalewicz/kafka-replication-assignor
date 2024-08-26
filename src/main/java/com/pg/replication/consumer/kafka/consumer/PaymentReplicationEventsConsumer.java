package com.pg.replication.consumer.kafka.consumer;

import com.pg.replication.common.event.*;
import com.pg.replication.consumer.payment.PaymentEventHandler;
import lombok.AllArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class PaymentReplicationEventsConsumer {

    private PaymentEventHandler paymentEventHandler;

    @KafkaListener(topics = "${kafka.topic.replication}", containerFactory = "kafkaListenerContainerFactory")
    public void listenToPaymentReplicaEvents(@Payload PaymentReplicaEvent event, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        System.out.println("Received Message of type: " + event.getClass().getName() + " from topicPartition: " + partition);
        handlePaymentEvent(event);
    }

    void handlePaymentEvent(PaymentReplicaEvent paymentEvent) {
        switch (paymentEvent) {
            case PaymentReplicaUpdatedEvent e -> paymentEventHandler.handlePaymentReplicaUpdatedEvent(e);
            case PaymentReplicaDeletedEvent e -> paymentEventHandler.handlePaymentReplicaDeletedEvent(e);
            case null, default -> System.out.println("null event");
        }
    }
}
