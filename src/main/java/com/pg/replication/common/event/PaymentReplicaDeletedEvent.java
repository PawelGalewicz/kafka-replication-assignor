package com.pg.replication.common.event;

import java.util.UUID;

public record PaymentReplicaDeletedEvent(UUID paymentUuid, Integer paymentSourcePartition) implements PaymentReplicaEvent {}
