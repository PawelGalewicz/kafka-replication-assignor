package com.pg.replication.consumer.lifecycle;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

public class ApplicationStateContext {
//    volatile is needed to always write changes from thread memory to main memory
    private volatile static ApplicationDetails applicationDetails = new ApplicationDetails();

    public static ApplicationState getState() {
        return applicationDetails.state;
    }

    public static ApplicationDetails getDetails() {
        return applicationDetails;
    }

    public static void stop() {
        applicationDetails.state = ApplicationState.TERMINATING;
    }

    public static void start() {
        applicationDetails.isNew = true;
    }

    public static void mature() {
        applicationDetails.isNew = false;
    }

    public static void stabilise() {
        applicationDetails.state = ApplicationState.STABLE;
    }

    public static void replicate() {
        applicationDetails.state = ApplicationState.REPLICATING;
    }

    public enum ApplicationState {
        STABLE,
        REPLICATING,
        TERMINATING
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Setter
    @Getter
    public static class ApplicationDetails implements Serializable {
        public ApplicationState state = ApplicationState.STABLE;
        public boolean isNew = true;
    }
}
