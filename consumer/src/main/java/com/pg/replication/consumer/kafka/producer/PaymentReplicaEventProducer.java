package com.pg.replication.consumer.kafka.producer;

import com.pg.replication.common.event.PaymentReplicaEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class PaymentReplicaEventProducer {

    private final KafkaTemplate<String, PaymentReplicaEvent> kafkaTemplate;

    private final String replicationTopicName;

    public PaymentReplicaEventProducer(KafkaTemplate<String, PaymentReplicaEvent> kafkaTemplate,
                                       @Value(value = "${kafka.topic.replica}") String replicationTopicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.replicationTopicName = replicationTopicName;
    }

    public void sendPaymentReplicationEvent(PaymentReplicaEvent paymentReplicaEvent) {

        CompletableFuture<SendResult<String, PaymentReplicaEvent>> future = kafkaTemplate.send(replicationTopicName, paymentReplicaEvent.paymentSourcePartition(), paymentReplicaEvent.paymentUuid().toString(), paymentReplicaEvent);
        future.whenComplete((result, ex) -> {

            if (ex == null) {
                log.debug("Sent message=[{}] with offset=[{}]", paymentReplicaEvent, result.getRecordMetadata().offset());
            } else {
                log.error("Unable to send message=[{}] due to : {}", paymentReplicaEvent, ex.getMessage());
            }
        });
    }

    public void clearPaymentReplicationEvent(Integer partition, UUID eventUuid) {

        CompletableFuture<SendResult<String, PaymentReplicaEvent>> future = kafkaTemplate.send(replicationTopicName, partition, eventUuid.toString(), null);
        future.whenComplete((result, ex) -> {

            if (ex == null) {
                log.debug("Cleared message with uuid=[{}]", eventUuid);
            } else {
                log.error("Unable to clear message with uuid=[{}] due to : {}", eventUuid, ex.getMessage());
            }
        });
    }
}
