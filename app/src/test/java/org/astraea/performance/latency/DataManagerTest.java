package org.astraea.performance.latency;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DataManagerTest {
  private final String topic = "topic-" + String.valueOf(System.currentTimeMillis());
  private final DataManager dataManager = DataManager.of(topic, 10);

  @Test
  void testRandomString() {
    var result =
        IntStream.range(0, 10)
            .mapToObj(i -> DataManager.randomString(10))
            .collect(Collectors.toSet());
    Assertions.assertEquals(10, result.size());
    result.forEach(r -> Assertions.assertEquals(10, r.length()));
  }

  @Test
  void testTakeRecord() {
    var record = dataManager.producerRecord();
    Assertions.assertEquals(topic, record.topic());
    Assertions.assertNotNull(record.key());
    Assertions.assertNotNull(record.value());
    Assertions.assertNotNull(record.headers());
  }

  @Test
  void testCompleteRecord() {
    Assertions.assertEquals(0, dataManager.numberOfProducerRecords());
    var record = dataManager.producerRecord();
    dataManager.sendingRecord(record, System.currentTimeMillis());
    Assertions.assertEquals(1, dataManager.numberOfProducerRecords());
    Assertions.assertEquals(record, dataManager.removeSendingRecord(record.key()).getKey());

    Assertions.assertThrows(
        NullPointerException.class, () -> dataManager.removeSendingRecord("aa".getBytes()));
  }
}
