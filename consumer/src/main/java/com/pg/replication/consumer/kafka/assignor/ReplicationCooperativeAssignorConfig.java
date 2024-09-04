package com.pg.replication.consumer.kafka.assignor;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class ReplicationCooperativeAssignorConfig extends AbstractConfig {

    public static final String MASTER_TOPIC = "assignment.consumer.topic.master";
    public static final String REPLICA_TOPIC = "assignment.consumer.topic.replica";
    public static final String INSTANCE_ID = "assignment.consumer.instance.id";
    public static final String MAX_ASSIGNMENTS_PER_INSTANCE = "assignment.consumer.instance.max_assignments";
    public static final String CONSUMER_PRIORITY_DOC = "The priority attached to the consumer that must be used for assigning partition. " +
            "Available partitions for subscribed topics are assigned to the consumer with the highest priority within the group.";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef()
                .define(MASTER_TOPIC, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, CONSUMER_PRIORITY_DOC)
                .define(REPLICA_TOPIC, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, CONSUMER_PRIORITY_DOC)
                .define(INSTANCE_ID, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, CONSUMER_PRIORITY_DOC)
//                fixme how to do ints in config
                .define(MAX_ASSIGNMENTS_PER_INSTANCE, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, CONSUMER_PRIORITY_DOC)
        ;
    }

    public ReplicationCooperativeAssignorConfig(final Map<?, ?> originals) {
        super(CONFIG, originals);
    }

    public String getMasterTopic() {
        return getString(MASTER_TOPIC);
    }

    public String getReplicaTopic() {
        return getString(REPLICA_TOPIC);
    }

    public String getInstanceId() {
        return getString(INSTANCE_ID);
    }

    public Integer getMaxAssignmentsPerInstance() {
        return Integer.valueOf(getString(MAX_ASSIGNMENTS_PER_INSTANCE));
    }

}