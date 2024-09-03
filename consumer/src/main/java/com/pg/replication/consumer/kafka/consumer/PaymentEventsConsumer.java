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
public class PaymentEventsConsumer {

    private PaymentEventHandler paymentEventHandler;

    @KafkaListener(topics = "${kafka.topic.master}", containerFactory = "kafkaListenerContainerFactory")
    public void listenToPaymentEvents(@Payload PaymentEvent event, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        System.out.println("Received Message of type: " + event.getClass().getName() + " from topicPartition: " + partition);
        handlePaymentEvent(event, partition);
    }

    void handlePaymentEvent(PaymentEvent paymentEvent, Integer sourcePartition) {
        switch (paymentEvent) {
            case PaymentCreatedEvent e -> paymentEventHandler.handlePaymentCreatedEvent(e, sourcePartition);
            case PaymentAuthorisedEvent e -> paymentEventHandler.handlePaymentAuthorisedEvent(e);
            case PaymentClearedEvent e -> paymentEventHandler.handlePaymentClearedEvent(e);
            case PaymentAmountChangedEvent e -> paymentEventHandler.handlePaymentAmountChangedEvent(e);
            case PaymentFinishedEvent e -> paymentEventHandler.handlePaymentFinishedEvent(e);
            case null, default -> System.out.println("null event");
        }
    }
}
