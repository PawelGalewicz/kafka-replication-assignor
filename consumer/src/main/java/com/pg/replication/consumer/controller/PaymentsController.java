package com.pg.replication.consumer.controller;

import com.pg.replication.common.model.Payment;
import com.pg.replication.consumer.payment.PaymentService;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController("/payments")
@AllArgsConstructor
public class PaymentsController {
    private final PaymentService paymentService;

    @GetMapping("/{uuid}")
    public Payment getPaymentByUuid(@PathVariable UUID uuid) {
        return paymentService.getPaymentByUuid(uuid);
    }
}
