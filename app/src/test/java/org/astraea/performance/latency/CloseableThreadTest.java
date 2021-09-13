package org.astraea.performance.latency;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CloseableThreadTest {

  @Test
  void testAllActions() throws InterruptedException {
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
}
