package org.astraea.performance;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AvgLatencyTest {
  @Test
  void testAverage() {
    Random rand = new Random();
    final int num = 1000;
    double avg = 0.0;
    AvgLatency avgLatency = new AvgLatency();

    Assertions.assertEquals(0, avgLatency.avg());

    for (int i = 0; i < num; ++i) {
      long next = rand.nextInt();
      avg += ((double) next - avg) / (i + 1);
      avgLatency.put(next);
    }

    Assertions.assertEquals(avg, avgLatency.avg());
  }

  @Test
  void testBytes() {
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final AvgLatency avgLatency = new AvgLatency();
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
              avgLatency.addBytes(input);
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
              longAdder.add(avgLatency.bytes());
            }
          }
        };
    adder.start();
    getter.start();
    countDownLatch.countDown();
    try {
      adder.join();
      longAdder.add(avgLatency.bytes());
    } catch (InterruptedException ie) {
    }
    Assertions.assertEquals(loopCount * input, longAdder.sum());
  }
}
