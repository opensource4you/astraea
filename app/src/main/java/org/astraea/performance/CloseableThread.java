package org.astraea.performance;

import java.io.Closeable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class CloseableThread extends Thread implements Closeable {
  protected AtomicBoolean closed = new AtomicBoolean(false);
  private CountDownLatch countDownLatch = new CountDownLatch(1);

  @Override
  public final void run() {
    try {
      while (!closed.get()) execute();
    } finally {
      cleanup();
      countDownLatch.countDown();
    }
  }

  public abstract void execute();

  @Override
  public void close() {
    closed.set(true);
    try {
      countDownLatch.await();
      // cleanup done
    } catch (InterruptedException e) {
    }
  }

  public void cleanup() {}
}
