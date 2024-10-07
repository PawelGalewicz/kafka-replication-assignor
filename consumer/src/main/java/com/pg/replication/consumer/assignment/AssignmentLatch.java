package com.pg.replication.consumer.assignment;

import com.pg.replication.consumer.kafka.rebalance.RebalanceService;
import com.pg.replication.consumer.store.InMemoryPartitionAssignmentStore;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class AssignmentLatch {

    private final InMemoryPartitionAssignmentStore partitionAssignmentStore;
    private final RebalanceService rebalanceService;

    public synchronized void cleanAssignment(Runnable callback) {
        while (partitionAssignmentStore.isAssignmentPresent()) {
            try {
                rebalanceService.forceRebalance();
                wait();
            } catch (InterruptedException e) {
//                need to interrupt the thread state in case any other process wanted to wait
                Thread.currentThread().interrupt();
                break;
            }
        }

        callback.run();
    }

    public synchronized void handleAssignmentChange() {
        notifyAll();
    }
}
