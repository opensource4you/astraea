package org.astraea.app.consumer.experiment;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

class Listener implements ConsumerRebalanceListener {

    private final int id;
    private long revokedTime = System.currentTimeMillis();
    private final KafkaConsumer<?, ?> consumer;
    private final Map<Integer, ConcurrentLinkedQueue<RebalanceTime>> generationIDTime;
    Listener(int id, KafkaConsumer<?, ?> consumer, Map<Integer, ConcurrentLinkedQueue<RebalanceTime>> generationIDTime) {
        this.consumer = consumer;
        this.id = id;
        this.generationIDTime = generationIDTime;
    }
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        revokedTime = System.currentTimeMillis();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("Assigned #" + id);
        long assignedTime = System.currentTimeMillis();
        Duration duration = Duration.ofMillis(assignedTime - revokedTime);
        RebalanceTime rebalanceTime = new RebalanceTime(duration, id);
        generationIDTime.putIfAbsent(consumer.groupMetadata().generationId(), new ConcurrentLinkedQueue<>());
        generationIDTime.get(consumer.groupMetadata().generationId()).add(rebalanceTime);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        System.out.println("#"+id + " lost");
    }
}
