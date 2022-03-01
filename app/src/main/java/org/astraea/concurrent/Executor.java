package org.astraea.concurrent;

@FunctionalInterface
public interface Executor extends AutoCloseable {

  static Executor of(Runnable runnable) {
    return () -> {
      runnable.run();
      return State.RUNNING;
    };
  }

  /**
   * @return the state of this executor
   * @throws InterruptedException This is an expected exception if your executor needs to call
   *     blocking method. This exception is not printed to console.
   */
  State execute() throws InterruptedException;

  /** close this executor. */
  @Override
  default void close() {}

  /**
   * If this executor is in blocking mode, this method offers a way to wake up executor to close.
   */
  default void wakeup() {}
}
