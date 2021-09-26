package org.astraea.performance;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

public class FakeComponentFactory implements ComponentFactory {
  public final AtomicInteger consumerClosed = new AtomicInteger(0);
  public final AtomicInteger producerClosed = new AtomicInteger(0);
  public final LongAdder produced = new LongAdder();
  public final HashMap<String, NewTopic> topicToConfig;

  public FakeComponentFactory() {
    topicToConfig = new HashMap<>();
  }

  @Override
  public Consumer createConsumer(Collection<String> topics) {
    return new Consumer() {
      @Override
      public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
        // return a record with (current time(ms) - 10ms).
        return new ConsumerRecords<>(
            Collections.singletonMap(
                new TopicPartition("", 0),
                Collections.singletonList(
                    new ConsumerRecord<>(
                        "",
                        0,
                        0,
                        System.currentTimeMillis() - 10,
                        TimestampType.NO_TIMESTAMP_TYPE,
                        0L,
                        5,
                        10,
                        new byte[5],
                        new byte[10],
                        new RecordHeaders()))));
      }

      @Override
      public void close() {
        consumerClosed.incrementAndGet();
      }
    };
  }

  @Override
  public Producer createProducer() {
    return new Producer() {
      @Override
      public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
        try {
          produced.increment();
          Thread.sleep(1);
        } catch (InterruptedException ie) {
        }
        return KafkaFuture.completedFuture(
            new RecordMetadata(
                new TopicPartition("", 0), -1, 0, System.currentTimeMillis() - 10, 0L, 5, 10));
      }

      @Override
      public void close() {
        producerClosed.incrementAndGet();
      }
    };
  }

  @Override
  public TopicAdmin createAdmin() {
    return new TopicAdmin() {
      @Override
      public Set<String> listTopics() {
        return topicToConfig.keySet();
      }

      @Override
      public Map<String, KafkaFuture<Void>> createTopics(Collection<NewTopic> topics) {
        HashMap<String, KafkaFuture<Void>> map = new HashMap<>();
        for (NewTopic topic : topics) {
          topicToConfig.put(topic.name(), topic);
          KafkaFuture<Void> future = KafkaFuture.completedFuture(null);
          map.put(topic.name(), future);
        }
        return map;
      }

      @Override
      public void close() {}

      @Override
      public List<TopicPartitionInfo> partitions(String topic) {
        List<TopicPartitionInfo> list = new ArrayList<>();
        for (int i = 0; i < topicToConfig.get(topic).numPartitions(); ++i)
          list.add(new TopicPartitionInfo(0, null, List.of(), List.of()));
        return list;
      }
    };
  }
}
