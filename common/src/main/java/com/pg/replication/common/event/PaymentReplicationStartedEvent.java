package com.pg.replication.common.event;

import java.util.UUID;

public record PaymentReplicationStartedEvent(UUID replicationProcessUuid, Integer paymentSourcePartition) implements PaymentReplicaEvent {

    @Override
    public UUID paymentUuid() {
        // paymentUuid set to replication process id as we don't associate this event with a specific payment
        return replicationProcessUuid;
    }
}
