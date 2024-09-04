package com.pg.replication.consumer.partition;

import com.pg.replication.consumer.payment.PaymentReplicaService;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.concurrent.ConcurrentSkipListSet;

@Service
@AllArgsConstructor
public class PartitionAssignmentService {

    private final ConcurrentSkipListSet<Integer> revokedMasterPartitions = new ConcurrentSkipListSet<>();
    private final ConcurrentSkipListSet<Integer> revokedReplicaPartitions = new ConcurrentSkipListSet<>();

    private final PaymentReplicaService paymentReplicaService;

    public void addAssignedMasterPartitions(Collection<Integer> masterPartitions) {
        handlePreviouslyRevokedReplicas(masterPartitions);
    }

    public void addAssignedReplicaPartitions(Collection<Integer> replicaPartitions) {
        handlePreviouslyRevokedMasters(replicaPartitions);
    }

    public void revokeMasterPartitions(Collection<Integer> masterPartitions) {
        revokedMasterPartitions.addAll(masterPartitions);
    }

    public void revokeReplicaPartitions(Collection<Integer> replicaPartitions) {
        revokedReplicaPartitions.addAll(replicaPartitions);
    }

    private void handlePreviouslyRevokedReplicas(Collection<Integer> masterPartitions) {
        revokedReplicaPartitions.removeAll(masterPartitions);
        revokedReplicaPartitions.forEach(paymentReplicaService::deleteReplicaPaymentsByPartition);
    }

    private void handlePreviouslyRevokedMasters(Collection<Integer> replicaPartitions) {
        revokedMasterPartitions.removeAll(replicaPartitions);
        revokedMasterPartitions.forEach(paymentReplicaService::deleteReplicaPaymentsByPartition);
    }
}
