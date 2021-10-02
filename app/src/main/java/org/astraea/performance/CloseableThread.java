package org.astraea.performance;

import java.io.Closeable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class CloseableThread extends Thread implements Closeable {
  protected final AtomicBoolean closed = new AtomicBoolean(false);
  private final CountDownLatch countDownLatch = new CountDownLatch(1);
  private final boolean executeOnce;
  private final AtomicLong threadId = new AtomicLong();

  /** Set this thread to iterate forever until close() is called */
  protected CloseableThread() {
    this(false);
  }

  /** Set this thread to iterate or run once. */
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
    } finally {
      cleanup();
      countDownLatch.countDown();
    }
  }

  /**
   * The process to iterate.
   */
  protected abstract void execute();

  @Override
  public void close() {
    if (threadId.get() == Thread.currentThread().getId()) {
      throw new RuntimeException("Should not call close() in execute().");
    }
    closed.set(true);
    try {
      countDownLatch.await();
      // cleanup done
    } catch (InterruptedException ignored) {
    }
  }

  /** cleanup() will be called before the thread end. */
  protected void cleanup() {}
}
