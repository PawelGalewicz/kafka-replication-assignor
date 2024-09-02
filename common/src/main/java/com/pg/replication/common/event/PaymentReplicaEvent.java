package com.pg.replication.common.event;

public interface PaymentReplicaEvent extends PaymentEvent {
    Integer paymentSourcePartition();
}
