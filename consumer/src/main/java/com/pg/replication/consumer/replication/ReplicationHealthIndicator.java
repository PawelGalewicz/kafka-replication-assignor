package com.pg.replication.consumer.replication;

import com.pg.replication.consumer.lifecycle.ApplicationStateContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import static com.pg.replication.consumer.lifecycle.ApplicationStateContext.ApplicationState.REPLICATING;

@Component
public class ReplicationHealthIndicator implements HealthIndicator {

    private final ReplicationProcessService replicationProcessService;

    private final String replicationTopicName;

    public ReplicationHealthIndicator(ReplicationProcessService replicationProcessService,
                                      @Value(value = "${kafka.topic.replica}")
                                      String replicationTopicName) {
        this.replicationProcessService = replicationProcessService;
        this.replicationTopicName = replicationTopicName;
    }

    @Override
    public Health health() {
        if (!REPLICATING.equals(ApplicationStateContext.getState())) {
            return Health.up().build();
        }

        Health.Builder health = Health.up();
        replicationProcessService.getCurrentReplicationProcesses()
                .forEach(replicationProcess ->
                        health.withDetail(String.format("%s-%s", replicationTopicName, replicationProcess.partition().toString()), replicationProcess.replicationProcessUuid()));

        return health.build();
    }
}
