package org.astraea.topic;

import java.util.Arrays;
import org.apache.kafka.common.record.CompressionType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CompressionTest {

  @Test
  void testNameOfKafka() {
    Assertions.assertEquals(Compression.values().length, CompressionType.values().length);
    Arrays.stream(Compression.values())
        .forEach(
            c ->
                Assertions.assertEquals(
                    c.nameOfKafka(), CompressionType.forName(c.nameOfKafka()).name));
  }
}
