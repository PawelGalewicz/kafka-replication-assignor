package com.pg.replication.consumer.lifecycle;

import com.pg.replication.consumer.assignment.AssignmentLatch;
import org.springframework.boot.web.context.WebServerGracefulShutdownLifecycle;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.stereotype.Component;

@Component
public class ApplicationLifecycle implements SmartLifecycle {

    private final AssignmentLatch assignmentCleanupLatch;
    private Boolean isRunning = false;

    public ApplicationLifecycle(AssignmentLatch assignmentCleanupLatch) {
        this.assignmentCleanupLatch = assignmentCleanupLatch;
    }

    @Override
    public void stop(Runnable callback) {
        ApplicationStateContext.stop();
        assignmentCleanupLatch.cleanAssignment(callback);
    }

    @Override
    public int getPhase() {
//        we want to trigger it before webserver stops accepting new requests
//         fixme might not be needed
        return AbstractMessageListenerContainer.DEFAULT_PHASE + 1;
    }

    @Override
    public void start() {
        ApplicationStateContext.start();
        isRunning = true;
    }

    @Override
    public void stop() {
        throw new UnsupportedOperationException("Stop must not be invoked directly");
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }
}
