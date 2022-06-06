package org.astraea.app.concurrent;

public enum State {
  /** this thread is done. ThreadPool will call close to release the thread. */
  DONE,
  /** this thread is running */
  RUNNING
}
