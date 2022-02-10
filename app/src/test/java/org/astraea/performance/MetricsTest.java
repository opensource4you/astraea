package org.astraea.performance;

import java.util.Random;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MetricsTest {

  @Test
  void testAverage() {
    Random rand = new Random();
    final int num = 1000;
    double avg = 0.0;
    Metrics metrics = new Metrics();

    Assertions.assertEquals(0, metrics.avgLatency());

    for (int i = 0; i < num; ++i) {
      long next = rand.nextInt();
      avg += ((double) next - avg) / (i + 1);
      metrics.accept(next, 0L);
    }

    Assertions.assertEquals(avg, metrics.avgLatency());
  }

  @Test
  void testBytes() {
    var metrics = new Metrics();

    Assertions.assertEquals(0, metrics.bytes());
    metrics.accept(0L, 1000L);
    Assertions.assertEquals(1000, metrics.bytes());
  }

  @Test
  void testCurrentBytes() {
    var metrics = new Metrics();

    Assertions.assertEquals(0, metrics.clearAndGetCurrentBytes());
    metrics.accept(0L, 100L);
    metrics.accept(0L, 101L);
    Assertions.assertEquals(201, metrics.clearAndGetCurrentBytes());
    Assertions.assertEquals(0, metrics.clearAndGetCurrentBytes());
  }
}
