package com.pg.replication.common.event;

import java.util.UUID;

public record PaymentAuthorisedEvent(UUID paymentUuid) implements PaymentEvent {}
