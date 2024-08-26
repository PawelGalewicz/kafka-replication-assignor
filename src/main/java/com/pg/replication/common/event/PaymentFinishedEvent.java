package com.pg.replication.common.event;

import java.util.UUID;

public record PaymentFinishedEvent(UUID paymentUuid) implements PaymentEvent {}
