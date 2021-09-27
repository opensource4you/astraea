package org.astraea.performance.latency;

import java.io.Closeable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

abstract class CloseableThread implements Runnable, Closeable {
  private final AtomicBoolean closed = new AtomicBoolean();
  private final CountDownLatch closeLatch = new CountDownLatch(1);
  private final boolean executeOnce;
  private final AtomicLong threadId = new AtomicLong();

  protected CloseableThread() {
    this(false);
  }

  protected CloseableThread(boolean executeOnce) {
    this.executeOnce = executeOnce;
  }

  @Override
  public final void run() {
    try {
      threadId.set(Thread.currentThread().getId());
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

  /** final action when leaving loop. */
  void cleanup() {}

  @Override
  public void close() {
    if (threadId.get() == Thread.currentThread().getId()) {
      throw new RuntimeException("Should not call close() in execute().");
    }
    closed.set(true);
    try {
      closeLatch.await();
    } catch (InterruptedException e) {
      // swallow
    }
  }
}
