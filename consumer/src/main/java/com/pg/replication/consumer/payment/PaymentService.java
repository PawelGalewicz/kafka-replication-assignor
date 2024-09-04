package com.pg.replication.consumer.payment;

import com.pg.replication.common.event.PaymentReplicaDeletedEvent;
import com.pg.replication.common.event.PaymentReplicaUpdatedEvent;
import com.pg.replication.common.event.PaymentReplicationStartedEvent;
import com.pg.replication.common.model.Payment;
import com.pg.replication.common.model.PaymentReplica;
import com.pg.replication.common.model.PaymentStatus;
import com.pg.replication.consumer.kafka.producer.PaymentReplicaEventProducer;
import com.pg.replication.consumer.store.InMemoryPaymentStore;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@AllArgsConstructor
public class PaymentService {
    private InMemoryPaymentStore paymentStore;
    private PaymentReplicaEventProducer paymentReplicaEventProducer;

    public void createPayment(UUID uuid, String from, String to, Integer amount, Integer sourcePartition) {
        Payment payment = new Payment(uuid, from, to, PaymentStatus.PENDING, amount, sourcePartition);
        paymentStore.insertPayment(payment);
        sendPaymentReplicaUpdatedEvent(payment);
    }

    public Payment getPaymentByUuid(UUID uuid) {
        return paymentStore.getPayment(uuid);
    }

    public synchronized void authorisePayment(UUID uuid) {
        Payment payment = paymentStore.getPayment(uuid);
        Payment newPayment = payment.toBuilder().status(PaymentStatus.AUTHORISED).build();
        updatePayment(newPayment);
    }

    public synchronized void clearPayment(UUID uuid) {
        Payment payment = paymentStore.getPayment(uuid);
        Payment newPayment = payment.toBuilder().status(PaymentStatus.CLEARED).build();
        updatePayment(newPayment);
    }

    public void changePaymentAmount(UUID uuid, Integer newAmount) {
        Payment payment = paymentStore.getPayment(uuid);
        Payment newPayment = payment.toBuilder().amount(newAmount).build();
        updatePayment(newPayment);
    }

    public void updatePayment(Payment payment) {
        paymentStore.updatePayment(payment);
        sendPaymentReplicaUpdatedEvent(payment);
    }

    public void deletePayment(UUID paymentUuid) {
        paymentStore.deletePayment(paymentUuid)
                .ifPresent(this::sendPaymentReplicaDeletedEvent);
    }

    private void sendPaymentReplicaUpdatedEvent(Payment payment) {
        PaymentReplica paymentReplica = PaymentReplica.fromPayment(payment);
        paymentReplicaEventProducer.sendPaymentReplicationEvent(new PaymentReplicaUpdatedEvent(payment.getPaymentUuid(), paymentReplica));
    }
    private void sendPaymentReplicaDeletedEvent(Payment payment) {
        paymentReplicaEventProducer.sendPaymentReplicationEvent(new PaymentReplicaDeletedEvent(payment.getPaymentUuid(), payment.getSourcePartition()));
    }
}
