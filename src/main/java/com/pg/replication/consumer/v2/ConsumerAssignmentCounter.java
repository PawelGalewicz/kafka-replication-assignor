package com.pg.replication.consumer.v2;

public class ConsumerAssignmentCounter {
    final String consumer;
    Integer numberOfMasters = 0;
    Integer numberOfReplicas = 0;

    public ConsumerAssignmentCounter(String consumer) {
        this.consumer = consumer;
    }

    public Integer getNumberOfMasters() {
        return numberOfMasters;
    }

    public Integer getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public Integer getNumberOfPartitions() {
        return numberOfMasters + numberOfReplicas;
    }

    public void incrementMasters() {
        numberOfMasters = numberOfReplicas + 1;
    }

    public void decrementMasters() {
        if (numberOfMasters == 0) {
            return;
        }

        numberOfMasters = numberOfReplicas - 1;
    }

    public void incrementReplicas() {
        numberOfReplicas = numberOfReplicas + 1;
    }

    public void decrementReplicas() {
        if (numberOfReplicas == 0) {
            return;
        }

        numberOfReplicas = numberOfReplicas - 1;
    }
}
