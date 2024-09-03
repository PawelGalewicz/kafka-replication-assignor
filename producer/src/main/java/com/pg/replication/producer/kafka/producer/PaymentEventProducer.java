package com.pg.replication.producer.kafka.producer;

import com.pg.replication.common.event.PaymentEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class PaymentEventProducer {

    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    private final String inputTopicName;

    public PaymentEventProducer(KafkaTemplate<String, PaymentEvent> kafkaTemplate,
                                @Value(value = "${kafka.topic.master}") String inputTopicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.inputTopicName = inputTopicName;
    }

    public void sendPaymentEvent(PaymentEvent paymentEvent) {

        CompletableFuture<SendResult<String, PaymentEvent>> future = kafkaTemplate.send(inputTopicName, paymentEvent.paymentUuid().toString(), paymentEvent);
        future.whenComplete((result, ex) -> {

            if (ex == null) {
                System.out.println("Sent message=[" + paymentEvent + "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" + paymentEvent + "] due to : " + ex.getMessage());
            }
        });
    }
}
