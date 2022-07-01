package org.astraea.app.consumer.experiment;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

class Consumer extends Thread {
    private final int id;
    private final KafkaConsumer<?, ?> consumer;
    private final Listener listener;
    private final Map<Integer, ConcurrentLinkedQueue<RebalanceTime>> timePerGeneration;
    Consumer(int id, KafkaConsumer<?, ?> consumer, Map<Integer, ConcurrentLinkedQueue<RebalanceTime>> timePerGeneration) {
        this.consumer = consumer;
        this.id = id;
        this.timePerGeneration = timePerGeneration;
        this.listener = new Listener(id, consumer, timePerGeneration);
    }

    public void doSubscribe(Set<String> topics) { consumer.subscribe(topics, listener); }

    @Override
    public void run() {
        try {
            System.out.println("Start consumer #"+id);
            while(!Thread.currentThread().isInterrupted()) {
                consumer.poll(Duration.ofMillis(250));
            }
        } catch (Exception e) {
            System.out.println("Close consumer #" + id);
            Thread.interrupted();
        } finally {
            consumer.close();
        }
    }
}
