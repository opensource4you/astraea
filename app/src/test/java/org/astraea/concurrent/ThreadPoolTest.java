package org.astraea.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ThreadPoolTest {

  private static class CountExecutor implements ThreadPool.Executor {

    private final AtomicInteger executeCount = new AtomicInteger();
    private final AtomicInteger closeCount = new AtomicInteger();
    private final AtomicInteger wakeupCount = new AtomicInteger();

    @Override
    public State execute() {
      executeCount.incrementAndGet();
      return State.RUNNING;
    }

    @Override
    public void close() {
      closeCount.incrementAndGet();
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
    Assertions.assertEquals(1, executor.closeCount.get());
    Assertions.assertEquals(1, executor.wakeupCount.get());
  }

  @Test
  void testWaitAll() {
    try (var pool = ThreadPool.builder().executor(() -> ThreadPool.Executor.State.DONE).build()) {
      pool.waitAll();
    }
  }
}
