package com.pg.replication.producer.kafka.producer;

import com.pg.replication.common.event.PaymentEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
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
                log.debug("Sent message=[{}] with offset=[{}]", paymentEvent, result.getRecordMetadata().offset());
            } else {
                log.error("Unable to send message=[{}] due to : {}", paymentEvent, ex.getMessage());
            }
        });
    }
}
