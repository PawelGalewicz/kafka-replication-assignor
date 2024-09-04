package com.pg.replication.consumer.replication;

import java.util.Comparator;
import java.util.UUID;

public record ReplicationProcess(UUID replicationProcessUuid, Integer partition) {

    public static Comparator<ReplicationProcess> comparator() {
        return Comparator.comparing(ReplicationProcess::partition)
                .thenComparing(ReplicationProcess::replicationProcessUuid);
    }
}
