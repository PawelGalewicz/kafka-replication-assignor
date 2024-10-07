package com.pg.replication.consumer.lifecycle;

public class ApplicationStateContext {
//    volatile is needed to always write changes from thread memory to main memory
    private volatile static ApplicationState applicationState = ApplicationState.STABLE;

    public static ApplicationState getState() {
        return applicationState;
    }

    public static void stop() {
        applicationState = ApplicationState.TERMINATING;
    }

    public static void start() {
        applicationState = ApplicationState.NEW;
    }

    public static void stabilise() {
        applicationState = ApplicationState.STABLE;
    }

    public static void replicate() {
        applicationState = ApplicationState.REPLICATING;
    }

    public enum ApplicationState {
        NEW,
        STABLE,
        REPLICATING,
        TERMINATING
    }
}
