package com.pg.replication.consumer.payment;

import com.pg.replication.common.model.Payment;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
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
        Map<UUID, Payment> paymentMap = paymentsMapPerPartition.get(payment.getSourcePartition());
        Payment previousVersion = paymentMap.get(payment.getPaymentUuid());
        if (!Objects.equals(previousVersion.getSourcePartition(), payment.getSourcePartition())) {
            paymentToPartition.replace(payment.getPaymentUuid(), payment.getSourcePartition());
        }
        return paymentMap.replace(payment.getPaymentUuid(), payment);
    }

    public Payment deletePayment(UUID paymentUuid) {
        Integer partition = paymentToPartition.get(paymentUuid);
        paymentToPartition.remove(paymentUuid);
        return paymentsMapPerPartition.get(partition).remove(paymentUuid);
    }

    public void prune(Integer partition) {
        Set<UUID> uuidsToRemove = paymentsMapPerPartition.get(partition).keySet();
        paymentsMapPerPartition.remove(partition);
        uuidsToRemove.forEach(paymentToPartition::remove);
    }
}
