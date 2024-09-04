package com.pg.replication.consumer.payment;

import com.pg.replication.common.event.*;
import com.pg.replication.consumer.replication.ReplicationProcessService;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class PaymentEventHandler {
    private final PaymentService paymentService;
    private final PaymentReplicaService paymentReplicaService;
    private final ReplicationProcessService replicationProcessService;

    public void handlePaymentCreatedEvent(PaymentCreatedEvent paymentCreatedEvent, Integer sourcePartition) {
        paymentService.createPayment(paymentCreatedEvent.paymentUuid(), paymentCreatedEvent.from(), paymentCreatedEvent.to(), paymentCreatedEvent.amount(), sourcePartition);
    }

    public void handlePaymentAuthorisedEvent(PaymentAuthorisedEvent paymentAuthorisedEvent) {
        paymentService.authorisePayment(paymentAuthorisedEvent.paymentUuid());
    }

    public void handlePaymentClearedEvent(PaymentClearedEvent paymentClearedEvent) {
        paymentService.clearPayment(paymentClearedEvent.paymentUuid());
    }

    public void handlePaymentAmountChangedEvent(PaymentAmountChangedEvent paymentAmountChangedEvent) {
        paymentService.changePaymentAmount(paymentAmountChangedEvent.paymentUuid(), paymentAmountChangedEvent.newAmount());
    }

    public void handlePaymentFinishedEvent(PaymentFinishedEvent paymentFinishedEvent) {
        paymentService.deletePayment(paymentFinishedEvent.paymentUuid());
    }

    public void handlePaymentReplicaUpdatedEvent(PaymentReplicaUpdatedEvent paymentReplicaUpdatedEvent) {
        paymentReplicaService.updateReplicaPayment(paymentReplicaUpdatedEvent.payment().toPayment());
    }

    public void handlePaymentReplicaDeletedEvent(PaymentReplicaDeletedEvent paymentReplicaDeletedEvent) {
        paymentReplicaService.deleteReplicaPayment(paymentReplicaDeletedEvent.paymentUuid());
    }

    public void handlePaymentReplicationStartedEvent(PaymentReplicationStartedEvent paymentReplicationStartedEvent) {
        replicationProcessService.stopReplicationProcess(paymentReplicationStartedEvent.replicationProcessUuid(), paymentReplicationStartedEvent.paymentSourcePartition());
    }
}
