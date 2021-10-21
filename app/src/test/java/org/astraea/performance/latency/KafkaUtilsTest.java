package org.astraea.performance.latency;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KafkaUtilsTest {

  @Test
  void testCreateHeader() {
    var header = KafkaUtils.header("a", "a".getBytes());
    Assertions.assertEquals("a", header.key());
    Assertions.assertArrayEquals("a".getBytes(), header.value());
  }

  @Test
  void testEqualOfHeader() {
    var headers0 =
        IntStream.range(0, 5)
            .mapToObj(i -> KafkaUtils.header(String.valueOf(i), String.valueOf(i).getBytes()))
            .collect(Collectors.toList());
    var headers1 =
        IntStream.range(0, 5)
            .mapToObj(i -> KafkaUtils.header(String.valueOf(i), String.valueOf(i).getBytes()))
            .collect(Collectors.toList());

    // reorder
    IntStream.range(0, 10)
        .forEach(
            i -> {
              Collections.shuffle(headers1);
              Collections.shuffle(headers0);
              Assertions.assertTrue(KafkaUtils.equal(headers0, headers0));
              Assertions.assertTrue(KafkaUtils.equal(headers1, headers1));
              Assertions.assertTrue(KafkaUtils.equal(headers0, headers1));
              Assertions.assertTrue(KafkaUtils.equal(headers1, headers0));
            });
  }

  @Test
  void testEqualOfRecord() {
    var topic = "topic";
    var key = "key".getBytes();
    var value = "value".getBytes();
    var headers = Collections.singleton(KafkaUtils.header("a", "b".getBytes()));
    var producerRecords =
        Arrays.asList(
            new ProducerRecord<>(topic, null, key, value, headers),
            new ProducerRecord<>(topic, null, 100L, key, value, headers));
    producerRecords.forEach(
        record -> Assertions.assertTrue(KafkaUtils.equal(record, toConsumerRecord(record))));
  }

  private static ConsumerRecord<byte[], byte[]> toConsumerRecord(
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
}
