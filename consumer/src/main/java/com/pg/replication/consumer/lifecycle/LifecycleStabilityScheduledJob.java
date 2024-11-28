package com.pg.replication.consumer.lifecycle;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class LifecycleStabilityScheduledJob {

    @Scheduled(initialDelayString = "${application.initialisation.delay-in-seconds}", timeUnit = TimeUnit.SECONDS)
    public void matureNewInstance() {
        ApplicationStateContext.mature();
    }
}
