package org.astraea.performance;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

public class FakeComponentFactory implements ComponentFactory {
  public final AtomicInteger consumerClosed = new AtomicInteger(0);
  public final AtomicInteger producerClosed = new AtomicInteger(0);
  public final AtomicInteger consumerPoll = new AtomicInteger(0);
  public final LongAdder produced = new LongAdder();
  public final AtomicInteger consumerWakeup = new AtomicInteger(0);

  @Override
  public Consumer createConsumer(Collection<String> topics) {
    return new Consumer() {
      @Override
      public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
        consumerPoll.incrementAndGet();
        byte[] value = new byte[1024];
        return new ConsumerRecords<>(
            Map.of(
                new TopicPartition("topic", 1),
                List.of(
                    new ConsumerRecord<byte[], byte[]>(
                        "topic",
                        1,
                        0,
                        System.currentTimeMillis() - 10,
                        TimestampType.NO_TIMESTAMP_TYPE,
                        0,
                        0,
                        1024,
                        null,
                        value))));
      }

      @Override
      public void wakeup() {
        consumerWakeup.incrementAndGet();
      }

      @Override
      public void cleanup() {
        consumerClosed.incrementAndGet();
      }
    };
  }

  @Override
  public Producer createProducer() {
    return new Producer() {
      @Override
      public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
        produced.increment();
        return KafkaFuture.completedFuture(
            new RecordMetadata(
                new TopicPartition("topic", 0),
                -1,
                0,
                System.currentTimeMillis() - 10,
                0L,
                0,
                1024));
      }

      @Override
      public void cleanup() {
        producerClosed.incrementAndGet();
      }
    };
  }
}
