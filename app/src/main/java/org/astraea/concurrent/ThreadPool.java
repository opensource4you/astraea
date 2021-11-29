package org.astraea.concurrent;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class offers a simple way to manage the threads. All threads added to builder are not
 * executing right now. Instead, all threads are starting when the pool is built.
 */
public interface ThreadPool extends AutoCloseable {

  /** close all running threads. */
  @Override
  void close();

  /** wait all executors to be done. */
  void waitAll();

  boolean isClosed();

  /** @return the number of threads */
  int size();

  static Builder builder() {
    return new Builder();
  }

  @FunctionalInterface
  interface Executor extends AutoCloseable {

    enum State {
      DONE,
      RUNNING
    }

    /**
     * @return the state of this executor
     * @throws InterruptedException This is an expected exception if your executor needs to call
     *     blocking method. This exception is not printed to console.
     */
    State execute() throws InterruptedException;

    /** close this executor. */
    default void close() {}

    /**
     * If this executor is in blocking mode, this method offers a way to wake up executor to close.
     */
    default void wakeup() {}
  }

  class Builder {
    private final List<Executor> executors = new ArrayList<>();

    private Builder() {}

    public Builder executor(Executor executor) {
      return executors(List.of(executor));
    }

    public Builder executors(Collection<Executor> executors) {
      this.executors.addAll(Objects.requireNonNull(executors));
      return this;
    }

    public ThreadPool build() {
      var closed = new AtomicBoolean(false);
      var latch = new CountDownLatch(executors.size());
      var service = Executors.newFixedThreadPool(executors.size());
      executors.forEach(
          executor ->
              service.execute(
                  () -> {
                    try {
                      while (!closed.get()) {
                        if (executor.execute() == Executor.State.DONE) break;
                      }
                    } catch (InterruptedException e) {
                      // swallow
                    } finally {
                      try {
                        executor.close();
                      } finally {
                        latch.countDown();
                      }
                    }
                  }));
      return new ThreadPool() {
        @Override
        public void close() {
          service.shutdown();
          closed.set(true);
          executors.forEach(Executor::wakeup);
          waitAll();
        }

        @Override
        public void waitAll() {
          try {
            latch.await();
          } catch (InterruptedException e) {
            // swallow
          }
        }

        @Override
        public boolean isClosed() {
          return closed.get();
        }

        @Override
        public int size() {
          return executors.size();
        }
      };
    }
  }
}
