package org.astraea.performance.latency;

import java.io.Closeable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

abstract class CloseableThread implements Runnable, Closeable {
  private final AtomicBoolean closed = new AtomicBoolean();
  private final CountDownLatch closeLatch = new CountDownLatch(1);
  private final boolean executeOnce;

  protected CloseableThread() {
    this(false);
  }

  protected CloseableThread(boolean executeOnce) {
    this.executeOnce = executeOnce;
  }

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

  /** final action when leaving loop. */
  void cleanup() {}

  @Override
  public void close() {
    if (StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
        .walk(
            s ->
                s.anyMatch(
                    stack ->
                        (stack.getMethodName().equals("execute"))
                            && (stack.getDeclaringClass() == this.getClass())))) {
      throw new RuntimeException();
    }
    closed.set(true);
    try {
      closeLatch.await();
    } catch (InterruptedException e) {
      // swallow
    }
  }
}
