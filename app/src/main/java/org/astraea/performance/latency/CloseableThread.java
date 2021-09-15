package org.astraea.performance.latency;

import java.io.Closeable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

abstract class CloseableThread implements Runnable, Closeable {
  private final AtomicBoolean closed = new AtomicBoolean();
  private final CountDownLatch closeLatch = new CountDownLatch(1);
  private boolean executeOnce = false;

  @Override
  public final void run() {
    try {
      do {
        execute();
      } while (!closed.get() && !executeOnce);
    } catch (InterruptedException e) {
      // swallow
    } finally {
      try {
        cleanup();
      } finally {
        closeLatch.countDown();
      }
    }
  }

  /** looped action. */
  abstract void execute() throws InterruptedException;

  /** set the thread before start then this thread executes one time. */
  public void executeOnce() {
    executeOnce = true;
  }

  /** final action when leaving loop. */
  void cleanup() {}

  @Override
  public void close() {
    closed.set(true);
    try {
      closeLatch.await();
    } catch (InterruptedException e) {
      // swallow
    }
  }
}
