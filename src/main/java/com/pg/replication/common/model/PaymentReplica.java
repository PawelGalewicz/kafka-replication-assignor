package com.pg.replication.common.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.UUID;

@NoArgsConstructor
@Setter
@Getter
public class PaymentReplica {
    private UUID paymentUuid;
    private String from;
    private String to;
    private PaymentStatus status;
    private Integer amount;
    private Integer sourcePartition;

    public Payment toPayment() {
        return Payment.builder()
                .paymentUuid(paymentUuid)
                .from(from)
                .to(to)
                .status(status)
                .amount(amount)
                .sourcePartition(sourcePartition)
                .build();
    }

    public static PaymentReplica fromPayment(Payment payment) {
        PaymentReplica paymentReplica = new PaymentReplica();
        paymentReplica.setPaymentUuid(payment.getPaymentUuid());
        paymentReplica.setFrom(payment.getFrom());
        paymentReplica.setTo(payment.getTo());
        paymentReplica.setStatus(payment.getStatus());
        paymentReplica.setAmount(payment.getAmount());
        paymentReplica.setSourcePartition(payment.getSourcePartition());
        return paymentReplica;
    }
}
