package com.pg.replication.producer.controller;

import com.pg.replication.common.event.*;
import com.pg.replication.producer.kafka.producer.PaymentEventProducer;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController("/payments")
@AllArgsConstructor
public class PaymentEventsController {

    private final PaymentEventProducer paymentEventProducer;

    @PostMapping("/create")
    public void createPayment(PaymentCreatedEvent paymentCreatedEvent) {
        paymentEventProducer.sendPaymentEvent(paymentCreatedEvent);
    }

    @PostMapping("/authorise")
    public void authorisePayment(PaymentAuthorisedEvent paymentAuthorisedEvent) {
        paymentEventProducer.sendPaymentEvent(paymentAuthorisedEvent);
    }

    @PostMapping("/clear")
    public void clearPayment(PaymentClearedEvent paymentClearedEvent) {
        paymentEventProducer.sendPaymentEvent(paymentClearedEvent);
    }

    @PostMapping("/change-amount")
    public void createPayment(PaymentAmountChangedEvent paymentAmountChangedEvent) {
        paymentEventProducer.sendPaymentEvent(paymentAmountChangedEvent);
    }

    @PostMapping("/finalise")
    public void finalisePayment(PaymentFinishedEvent paymentFinishedEvent) {
        paymentEventProducer.sendPaymentEvent(paymentFinishedEvent);
    }
}
