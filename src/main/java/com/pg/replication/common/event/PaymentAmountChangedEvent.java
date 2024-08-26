package com.pg.replication.common.event;

import java.util.UUID;

public record PaymentAmountChangedEvent(UUID paymentUuid, Integer newAmount) implements PaymentEvent {}
