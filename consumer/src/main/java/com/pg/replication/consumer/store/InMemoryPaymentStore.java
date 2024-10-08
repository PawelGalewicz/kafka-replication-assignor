package com.pg.replication.consumer.store;

import com.pg.replication.common.model.Payment;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class InMemoryPaymentStore {
    private final Map<Integer, Map<UUID, Payment>> paymentsMapPerPartition = new ConcurrentHashMap<>();
    private final Map<UUID, Integer> paymentToPartition = new ConcurrentHashMap<>();

    public Payment getPayment(UUID uuid) {
        Integer partition = paymentToPartition.get(uuid);
        return paymentsMapPerPartition.get(partition).get(uuid);
    }

    public Payment insertPayment(Payment payment) {
        Map<UUID, Payment> payments = paymentsMapPerPartition.computeIfAbsent(payment.getSourcePartition(), ignore -> new ConcurrentHashMap<>());
        paymentToPartition.put(payment.getPaymentUuid(), payment.getSourcePartition());
        return payments.put(payment.getPaymentUuid(), payment);
    }

    public Payment updatePayment(Payment payment) {
        Integer previousPartition = paymentToPartition.get(payment.getPaymentUuid());

        if(previousPartition == null) {
            return insertPayment(payment);
        }

        if (previousPartition.equals(payment.getSourcePartition())) {
            paymentsMapPerPartition.get(previousPartition).replace(payment.getPaymentUuid(), payment);
            return payment;
        } else {
            paymentToPartition.remove(payment.getPaymentUuid());
            paymentsMapPerPartition.get(previousPartition).remove(payment.getPaymentUuid());
            return insertPayment(payment);
        }
    }

    public Optional<Payment> deletePayment(UUID paymentUuid) {
        Integer partition = paymentToPartition.get(paymentUuid);
        if (partition == null) {
            return Optional.empty();
        }

        paymentToPartition.remove(paymentUuid);
        return Optional.of(paymentsMapPerPartition.get(partition).remove(paymentUuid));
    }

    public void prune(Integer partition) {
        if (paymentsMapPerPartition.containsKey(partition)) {
            Set<UUID> uuidsToRemove = paymentsMapPerPartition.get(partition).keySet();
            paymentsMapPerPartition.remove(partition);
            uuidsToRemove.forEach(paymentToPartition::remove);
        }
    }
}
