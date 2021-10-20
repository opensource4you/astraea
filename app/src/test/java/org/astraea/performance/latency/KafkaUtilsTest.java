package org.astraea.performance.latency;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.ProducerRecord;
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
        record ->
            Assertions.assertTrue(
                KafkaUtils.equal(record, FakeComponentFactory.toConsumerRecord(record))));
  }
}
