package com.pg.replication.consumer.store;

import com.pg.replication.consumer.replication.ReplicationProcess;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentSkipListSet;

@Service
public class InMemoryReplicationProcessStore {
    private final ConcurrentSkipListSet<ReplicationProcess> replicationProcessesSet = new ConcurrentSkipListSet<>(ReplicationProcess.comparator());

    public void addReplicationProcess(ReplicationProcess replicationProcess) {
        replicationProcessesSet.add(replicationProcess);
    }

    public void removeReplicationProcess(ReplicationProcess replicationProcess) {
        replicationProcessesSet.remove(replicationProcess);
    }

    public boolean containsReplicationProcess(ReplicationProcess replicationProcess) {
        return replicationProcessesSet.contains(replicationProcess);
    }

    public boolean isNotEmpty() {
        return !replicationProcessesSet.isEmpty();
    }
}
