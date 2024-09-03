package com.pg.replication.consumer.kafka.producer;

import com.pg.replication.common.event.PaymentReplicaEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
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
                System.out.println("Sent message=[" + paymentReplicaEvent + "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" + paymentReplicaEvent + "] due to : " + ex.getMessage());
            }
        });
    }
}
