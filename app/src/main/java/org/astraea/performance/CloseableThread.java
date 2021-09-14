package org.astraea.performance;

import java.io.Closeable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class CloseableThread extends Thread implements Closeable {
  protected final AtomicBoolean closed = new AtomicBoolean(false);
  private final CountDownLatch countDownLatch = new CountDownLatch(1);

  @Override
  public final void run() {
    try {
      while (!closed.get()) execute();
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
