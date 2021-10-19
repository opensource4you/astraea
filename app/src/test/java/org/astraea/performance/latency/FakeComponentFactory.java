package org.astraea.performance.latency;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.astraea.topic.FakeTopicAdmin;
import org.astraea.topic.TopicAdmin;

class FakeComponentFactory implements ComponentFactory {

  static ConsumerRecord<byte[], byte[]> toConsumerRecord(
      ProducerRecord<byte[], byte[]> producerRecord) {
    return new ConsumerRecord<>(
        producerRecord.topic(),
        1,
        1L,
        producerRecord.timestamp() == null
            ? System.currentTimeMillis()
            : producerRecord.timestamp(),
        TimestampType.CREATE_TIME,
        1L,
        producerRecord.key() == null ? 0 : producerRecord.key().length,
        producerRecord.value() == null ? 0 : producerRecord.value().length,
        producerRecord.key(),
        producerRecord.value(),
        new RecordHeaders(producerRecord.headers()));
  }

  final AtomicInteger producerSendCount = new AtomicInteger();
  final AtomicInteger producerFlushCount = new AtomicInteger();
  final AtomicInteger producerCloseCount = new AtomicInteger();

  final AtomicInteger consumerPoolCount = new AtomicInteger();
  final AtomicInteger consumerWakeupCount = new AtomicInteger();
  final AtomicInteger consumerCloseCount = new AtomicInteger();

  final AtomicInteger topicAdminListCount = new AtomicInteger();
  final AtomicInteger topicAdminCreateCount = new AtomicInteger();
  final AtomicInteger topicAdminCloseCount = new AtomicInteger();

  /** save all records from producer */
  final BlockingQueue<ProducerRecord<byte[], byte[]>> allRecords = new LinkedBlockingQueue<>();

  /** save non-consumed records */
  final BlockingQueue<ProducerRecord<byte[], byte[]>> records = new LinkedBlockingQueue<>();

  @Override
  public Producer producer() {
    return new Producer() {

      @Override
      public CompletionStage<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
        producerSendCount.incrementAndGet();
        allRecords.add(record);
        records.add(record);
        return CompletableFuture.completedFuture(
            new RecordMetadata(
                new TopicPartition(record.topic(), 1),
                1L,
                1L,
                1L,
                1L,
                record.key() == null ? 0 : record.key().length,
                record.value() == null ? 0 : record.value().length));
      }

      @Override
      public void flush() {
        producerFlushCount.incrementAndGet();
      }

      @Override
      public void close() {
        producerCloseCount.incrementAndGet();
      }
    };
  }

  @Override
  public Consumer consumer() {
    return new Consumer() {

      @Override
      public ConsumerRecords<byte[], byte[]> poll() {
        consumerPoolCount.incrementAndGet();
        try {
          var producerRecord = records.poll(1, TimeUnit.SECONDS);
          if (producerRecord == null) return new ConsumerRecords<>(Collections.emptyMap());
          return new ConsumerRecords<>(
              Collections.singletonMap(
                  new TopicPartition(producerRecord.topic(), 1),
                  Collections.singletonList(toConsumerRecord(producerRecord))));
        } catch (InterruptedException e) {
          throw new WakeupException();
        }
      }

      @Override
      public void wakeup() {
        consumerWakeupCount.incrementAndGet();
      }

      @Override
      public void close() {
        consumerCloseCount.incrementAndGet();
      }
    };
  }

  @Override
  public TopicAdmin topicAdmin() {
    return new FakeTopicAdmin() {

      @Override
      public Set<String> topics() {
        topicAdminListCount.incrementAndGet();
        return Collections.emptySet();
      }

      @Override
      public void createTopic(String topic, int numberOfPartitions) {
        topicAdminCreateCount.incrementAndGet();
      }

      @Override
      public void close() {
        topicAdminCloseCount.incrementAndGet();
      }
    };
  }
}
