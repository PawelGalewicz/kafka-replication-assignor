package com.pg.replication.consumer.kafka.rebalance;

import lombok.AllArgsConstructor;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

import static com.pg.replication.consumer.kafka.consumer.PaymentReplicationEventsConsumer.REPLICA_LISTENER_ID;

@Service
@AllArgsConstructor
public class RebalanceService {

//    ignore bean error, IntelliJ doesn't see it, but it is actually there
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    private final KafkaListenerEndpointRegistry registry;

    public void forceRebalance() {
        MessageListenerContainer listenerContainer = registry.getListenerContainer(REPLICA_LISTENER_ID);
        if (listenerContainer != null) {
            listenerContainer.enforceRebalance();
        }
    }
}
