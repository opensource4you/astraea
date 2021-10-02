package org.astraea.performance;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;
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
      metrics.putLatency(next);
    }

    Assertions.assertEquals(avg, metrics.avgLatency());
  }

  // Simultaneously add and get
  @Test
  void testBytes() {
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final Metrics metrics = new Metrics();
    final LongAdder longAdder = new LongAdder();
    final long input = 100;
    final int loopCount = 10000;
    Thread adder =
        new Thread() {
          @Override
          public void run() {
            try {
              countDownLatch.await();
            } catch (InterruptedException ie) {
            }
            for (int i = 0; i < loopCount; ++i) {
              metrics.addBytes(input);
            }
          }
        };
    Thread getter =
        new Thread() {
          @Override
          public void run() {
            try {
              countDownLatch.await();
            } catch (InterruptedException ie) {
            }
            for (int i = 0; i < loopCount; ++i) {
              longAdder.add(metrics.bytesThenReset());
            }
          }
        };
    adder.start();
    getter.start();
    countDownLatch.countDown();
    try {
      adder.join();
      longAdder.add(metrics.bytesThenReset());
    } catch (InterruptedException ie) {
    }
    Assertions.assertEquals(loopCount * input, longAdder.sum());
  }

  @Test
  public void testReset() {
    final Metrics metrics = new Metrics();
    metrics.addBytes(10);
    metrics.putLatency(11);

    metrics.reset();

    Assertions.assertEquals(0, metrics.bytesThenReset());
    Assertions.assertEquals(0, metrics.avgLatency());
  }
}
