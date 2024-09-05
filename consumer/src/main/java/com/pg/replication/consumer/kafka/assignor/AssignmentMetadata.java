package com.pg.replication.consumer.kafka.assignor;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.time.Instant;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
public class AssignmentMetadata implements Serializable {
    public String instance;
    public Instant instanceCreatedAt;
}
