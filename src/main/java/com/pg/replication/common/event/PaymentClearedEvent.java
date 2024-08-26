package com.pg.replication.common.event;

import java.util.UUID;

public record PaymentClearedEvent(UUID paymentUuid) implements PaymentEvent {}
