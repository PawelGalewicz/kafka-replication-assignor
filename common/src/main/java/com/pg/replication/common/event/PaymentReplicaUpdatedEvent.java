package com.pg.replication.common.event;

import com.pg.replication.common.model.PaymentReplica;

import java.util.UUID;

public record PaymentReplicaUpdatedEvent(UUID paymentUuid, PaymentReplica payment) implements PaymentReplicaEvent {
    @Override
    public Integer paymentSourcePartition() {
        return payment.getSourcePartition();
    }
}
