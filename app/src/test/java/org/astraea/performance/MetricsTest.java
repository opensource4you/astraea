package org.astraea.performance;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.astraea.concurrent.ThreadPool;
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
    final Metrics metrics = new Metrics();
    final LongAdder longAdder = new LongAdder();
    final AtomicInteger adderCount = new AtomicInteger(0);
    final AtomicInteger getterCount = new AtomicInteger(0);
    final long input = 100;
    final int loopCount = 10000;

    try (ThreadPool threadPool =
        ThreadPool.builder()
            .executor(
                () -> {
                  metrics.addBytes(input);
                  if (adderCount.incrementAndGet() < loopCount)
                    return ThreadPool.Executor.State.RUNNING;
                  else return ThreadPool.Executor.State.DONE;
                })
            .executor(
                () -> {
                  longAdder.add(metrics.bytesThenReset());
                  if (getterCount.incrementAndGet() < loopCount)
                    return ThreadPool.Executor.State.RUNNING;
                  else return ThreadPool.Executor.State.DONE;
                })
            .build()) {
      threadPool.waitAll();
    }
    longAdder.add(metrics.bytesThenReset());

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
