package com.pg.replication.consumer.lifecycle;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

import static com.pg.replication.consumer.lifecycle.ApplicationStateContext.ApplicationState.NEW;

@Component
public class LifecycleStabilityScheduledJob {

    @Scheduled(initialDelayString = "${application.initialisation.delay-in-seconds}", timeUnit = TimeUnit.SECONDS)
    public void stabiliseNewInstance() {
        if (NEW.equals(ApplicationStateContext.getState())) {
            ApplicationStateContext.stabilise();
        }
    }
}
