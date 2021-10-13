package org.astraea.partitioner.nodeLoadMetric;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SingleThreadPoolTest {

  private static class CountExecutor implements SingleThreadPool.Executor {

    private final AtomicInteger executeCount = new AtomicInteger();
    private final AtomicInteger cleanupCount = new AtomicInteger();

    @Override
    public void execute() {
      executeCount.incrementAndGet();
    }

    @Override
    public void cleanup() {
      cleanupCount.incrementAndGet();
    }
  }

  @Test
  void testSubmitThread() throws Exception {
    var executor = new CountExecutor();
    try (var pool = SingleThreadPool.builder().build(executor)) {
      TimeUnit.SECONDS.sleep(2);
    }
    Assertions.assertTrue(executor.executeCount.get() > 0);
    Assertions.assertEquals(1, executor.cleanupCount.get());
  }
}
