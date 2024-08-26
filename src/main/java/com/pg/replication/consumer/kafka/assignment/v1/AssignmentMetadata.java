package com.pg.replication.consumer.kafka.assignment.v1;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
public class AssignmentMetadata implements Serializable {
    public String instance;
}
