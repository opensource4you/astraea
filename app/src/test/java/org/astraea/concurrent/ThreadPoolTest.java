package org.astraea.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ThreadPoolTest {

  private static class CountExecutor implements ThreadPool.Executor {

    private final AtomicInteger executeCount = new AtomicInteger();
    private final AtomicInteger cleanupCount = new AtomicInteger();
    private final AtomicInteger wakeupCount = new AtomicInteger();

    @Override
    public void execute() {
      executeCount.incrementAndGet();
    }

    @Override
    public void cleanup() {
      cleanupCount.incrementAndGet();
    }

    @Override
    public void wakeup() {
      wakeupCount.incrementAndGet();
    }
  }

  @Test
  void testSubmitThread() throws Exception {
    var executor = new CountExecutor();
    try (var pool = ThreadPool.builder().executor(executor).build()) {
      TimeUnit.SECONDS.sleep(2);
    }
    Assertions.assertTrue(executor.executeCount.get() > 0);
    Assertions.assertEquals(1, executor.cleanupCount.get());
    Assertions.assertEquals(1, executor.wakeupCount.get());
  }

  @Test
  void testLoop() throws InterruptedException {
    var executor = new CountExecutor();
    try (var pool = ThreadPool.builder().executor(executor).loop(3).build()) {
      TimeUnit.SECONDS.sleep(3);
    }
    Assertions.assertEquals(3, executor.executeCount.get());
    Assertions.assertEquals(1, executor.cleanupCount.get());
    Assertions.assertEquals(1, executor.wakeupCount.get());
  }
}
