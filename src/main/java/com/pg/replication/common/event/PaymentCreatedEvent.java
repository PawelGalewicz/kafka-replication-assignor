package com.pg.replication.common.event;

import java.util.UUID;

public record PaymentCreatedEvent(UUID paymentUuid, String from, String to, Integer amount) implements PaymentEvent {}
