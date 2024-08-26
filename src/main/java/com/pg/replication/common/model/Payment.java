package com.pg.replication.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.UUID;

@AllArgsConstructor
@Getter
@Builder(toBuilder = true)
public class Payment {
    private final UUID paymentUuid;
    private final String from;
    private final String to;
    private PaymentStatus status;
    private Integer amount;
    private Integer sourcePartition;
}
