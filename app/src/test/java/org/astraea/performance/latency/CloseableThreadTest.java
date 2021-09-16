package org.astraea.performance.latency;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CloseableThreadTest {

  @Test
  void testExecuteAndCleanup() throws InterruptedException {
    var executeCount = new AtomicInteger();
    var cleanupCount = new AtomicInteger();
    var thread =
        new CloseableThread() {

          @Override
          void execute() {
            executeCount.incrementAndGet();
          }

          @Override
          void cleanup() {
            cleanupCount.incrementAndGet();
          }
        };

    var service = Executors.newSingleThreadExecutor();
    service.execute(thread);

    TimeUnit.SECONDS.sleep(1);

    thread.close();
    service.shutdown();
    Assertions.assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));

    Assertions.assertTrue(executeCount.get() > 0);
    Assertions.assertEquals(1, cleanupCount.get());
  }

  @Test
  void testThrowException() {
    var thread =
        new CloseableThread() {
          @Override
          void execute() {
            close();
          }
        };

    Assertions.assertEquals(
        "Should not call close() in execute().",
        Assertions.assertThrows(RuntimeException.class, thread::run).getMessage());
  }

  @Test
  void testExecuteOnce() throws InterruptedException {
    var executeCount = new AtomicInteger();
    var thread =
        new CloseableThread(true) {
          @Override
          void execute() {
            executeCount.incrementAndGet();
          }
        };

    var service = Executors.newSingleThreadExecutor();
    service.execute(thread);
    service.shutdown();

    Assertions.assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));

    Assertions.assertEquals(1, executeCount.get());
  }
}
