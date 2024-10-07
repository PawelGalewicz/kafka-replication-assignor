package com.pg.replication.consumer.replication;

import com.pg.replication.common.event.PaymentReplicationStartedEvent;
import com.pg.replication.consumer.kafka.producer.PaymentReplicaEventProducer;
import com.pg.replication.consumer.kafka.rebalance.RebalanceService;
import com.pg.replication.consumer.lifecycle.ApplicationStateContext;
import com.pg.replication.consumer.store.InMemoryReplicationProcessStore;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.UUID;

@Service
@AllArgsConstructor
public class ReplicationProcessService {

    private final InMemoryReplicationProcessStore replicationProcessStore;
    private final PaymentReplicaEventProducer paymentReplicaEventProducer;
    private final RebalanceService rebalanceService;

    public void startReplicationProcess(Integer partition) {
        UUID replicationProcessUuid = UUID.randomUUID();
        replicationProcessStore.addReplicationProcess(new ReplicationProcess(replicationProcessUuid, partition));
        sendPaymentReplicationStartedEvent(replicationProcessUuid, partition);
        ApplicationStateContext.replicate();
    }

    public void stopReplicationProcess(UUID replicationProcessUuid, Integer partition) {
        ReplicationProcess replicationProcess = new ReplicationProcess(replicationProcessUuid, partition);
        if (replicationProcessStore.containsReplicationProcess(replicationProcess)) {
            replicationProcessStore.removeReplicationProcess(replicationProcess);
            paymentReplicaEventProducer.clearPaymentReplicationEvent(partition, replicationProcessUuid);
            if (!isReplicationInProgress()) {
//            if every assigned replica partition was successfully consumed, then the replica node is ready for potential promotion to master,
//            so we trigger a rebalance to check if it is needed
                ApplicationStateContext.stabilise();
                rebalanceService.forceRebalance();
            }
        }
    }

    public boolean isReplicationInProgress() {
        return replicationProcessStore.isNotEmpty();
    }

    public Collection<ReplicationProcess> getCurrentReplicationProcesses() {
        return replicationProcessStore.getCurrentReplicationProcesses();
    }

    private void sendPaymentReplicationStartedEvent(UUID replicationProcessUuid, Integer partition) {
        paymentReplicaEventProducer.sendPaymentReplicationEvent(new PaymentReplicationStartedEvent(replicationProcessUuid, partition));
    }

}
