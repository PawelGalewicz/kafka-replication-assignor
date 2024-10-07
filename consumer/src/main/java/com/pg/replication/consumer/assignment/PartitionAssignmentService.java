package com.pg.replication.consumer.assignment;

import com.pg.replication.consumer.payment.PaymentReplicaService;
import com.pg.replication.consumer.store.InMemoryPartitionAssignmentStore;
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
    private final InMemoryPartitionAssignmentStore partitionAssignmentStore;
    private final AssignmentLatch assignmentLatch;

    public void addAssignedMasterPartitions(Collection<Integer> masterPartitions) {
        handlePreviouslyRevokedReplicas(masterPartitions);
        partitionAssignmentStore.addMasterPartitions(masterPartitions);
        assignmentLatch.handleAssignmentChange();
    }

    public void addAssignedReplicaPartitions(Collection<Integer> replicaPartitions) {
        handlePreviouslyRevokedMasters(replicaPartitions);
        partitionAssignmentStore.addReplicaPartitions(replicaPartitions);
        assignmentLatch.handleAssignmentChange();
    }

    public void revokeMasterPartitions(Collection<Integer> masterPartitions) {
        revokedMasterPartitions.addAll(masterPartitions);
        partitionAssignmentStore.removeMasterPartitions(masterPartitions);
    }

    public void revokeReplicaPartitions(Collection<Integer> replicaPartitions) {
        revokedReplicaPartitions.addAll(replicaPartitions);
        partitionAssignmentStore.removeReplicaPartitions(replicaPartitions);
    }

    private void handlePreviouslyRevokedReplicas(Collection<Integer> masterPartitions) {
        revokedReplicaPartitions.removeAll(masterPartitions);
        revokedReplicaPartitions.forEach(paymentReplicaService::deleteReplicaPaymentsByPartition);
        revokedReplicaPartitions.clear();
    }

    private void handlePreviouslyRevokedMasters(Collection<Integer> replicaPartitions) {
        revokedMasterPartitions.removeAll(replicaPartitions);
        revokedMasterPartitions.forEach(paymentReplicaService::deleteReplicaPaymentsByPartition);
        revokedMasterPartitions.clear();
    }
}
