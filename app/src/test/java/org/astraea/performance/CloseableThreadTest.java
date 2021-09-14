package org.astraea.performance;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CloseableThreadTest {
  @Test
  void testAllActions() {
    final AtomicBoolean signal = new AtomicBoolean(false);
    CloseableThread thread =
        new CloseableThread() {
          @Override
          protected void execute() {
            signal.set(true);
          }

          @Override
          protected void cleanup() {
            signal.set(false);
          }
        };
    thread.start();

    // Let the thread run
    try {
      Thread.sleep(1);
    } catch (InterruptedException ignored) {
    }

    Assertions.assertTrue(signal.get());
    thread.close();
    Assertions.assertFalse(signal.get());
  }
}
