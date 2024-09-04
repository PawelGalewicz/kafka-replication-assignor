package com.pg.replication.consumer.kafka.assignor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;



class InstanceAssignmentContainerTest {

    private InstanceAssignmentContainer.SortedLinkedList<Integer> list;

    @BeforeEach
    public void setup() {
        list = new InstanceAssignmentContainer.SortedLinkedList<>(Integer::compareTo);
    }

    @Test
    public void test1() {
        list.add(1);

        assertThat(list.size()).isEqualTo(1);

        assertThat(list.get(0)).isEqualTo(1);
    }

    @Test
    public void test2() {
        list.add(1);
        list.add(3);
        list.add(2);
        list.add(0);

        assertThat(list.size()).isEqualTo(4);

        assertThat(list.get(0)).isEqualTo(0);
        assertThat(list.get(1)).isEqualTo(1);
        assertThat(list.get(2)).isEqualTo(2);
        assertThat(list.get(3)).isEqualTo(3);
    }

    @Test
    public void test3() {
        list.add(1);
        list.add(3);
        list.add(2);
        list.remove(0);
        list.add(0);

        assertThat(list.size()).isEqualTo(3);

        assertThat(list.get(0)).isEqualTo(0);
        assertThat(list.get(1)).isEqualTo(2);
        assertThat(list.get(2)).isEqualTo(3);
    }

}