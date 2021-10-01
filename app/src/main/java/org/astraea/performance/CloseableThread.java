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
    } finally {
      cleanup();
      countDownLatch.countDown();
    }
  }

  /**
   * DO NOT call close() in execute()! It will cause deadlock. Use closed.set(true) in execute() to
   * stop the thread looping.
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
