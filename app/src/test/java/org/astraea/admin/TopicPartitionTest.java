package org.astraea.admin;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TopicPartitionTest {

  @Test
  void testConversion() {
    var ourTp = new TopicPartition("abc", 100);
    var kafkaTp = TopicPartition.to(ourTp);
    Assertions.assertEquals(ourTp.topic(), kafkaTp.topic());
    Assertions.assertEquals(ourTp.partition(), kafkaTp.partition());
  }

  @Test
  void testComparison() {
    var tp0 = new TopicPartition("abc", 100);
    var tp1 = new TopicPartition("abc", 101);
    Assertions.assertEquals(-1, tp0.compareTo(tp1));
    Assertions.assertEquals(1, tp1.compareTo(tp0));
    Assertions.assertEquals(0, tp0.compareTo(tp0));
    Assertions.assertEquals(tp0, tp0);
  }

  @Test
  void testString() {
    Assertions.assertEquals(0, TopicPartition.of("a-a", "0").partition());
    Assertions.assertThrows(IllegalArgumentException.class, () -> TopicPartition.of("a", "a"));

    Assertions.assertEquals(0, TopicPartition.of("a-a-0").partition());
    Assertions.assertThrows(IllegalArgumentException.class, () -> TopicPartition.of("a-a"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TopicPartition.of("a-a-"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TopicPartition.of("aaa"));
  }
}
