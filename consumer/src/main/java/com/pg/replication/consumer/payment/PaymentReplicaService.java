package com.pg.replication.consumer.payment;

import com.pg.replication.common.model.Payment;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@AllArgsConstructor
public class PaymentReplicaService {
    private InMemoryPaymentStore paymentStore;

    public void updateReplicaPayment(Payment payment) {
        paymentStore.updatePayment(payment);
    }

    public void deleteReplicaPayment(UUID paymentUuid) {
        paymentStore.deletePayment(paymentUuid);
    }
}
