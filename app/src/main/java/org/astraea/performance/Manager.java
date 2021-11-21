package org.astraea.performance;

import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/** Thread safe */
public class Manager {
  private final long records;
  private final boolean fixedSize;
  private final int size;
  private final CountDownLatch getAssignment;
  private final Random rand = new Random();

  // Random payload produced by this instance.
  private final AtomicLong produced = new AtomicLong(0L);
  // Record the records consumed by consumer.
  private final AtomicLong consumed = new AtomicLong(0L);
  private final long end;

  /**
   * Used to manage producing/consuming.
   *
   * @param records number of records at most to produce
   * @param duration execution time (includes initialization time)
   * @param fixedSize whether the length of the record should be random
   * @param size the length/(bound of random length) of the record
   */
  public Manager(long records, Duration duration, boolean fixedSize, int size, int consumers) {
    this.records = records;
    this.fixedSize = fixedSize;
    this.size = size;
    this.end = System.currentTimeMillis() + duration.toMillis();
    this.getAssignment = new CountDownLatch(consumers);
  }

  /**
   * Generate random byte array in random/fixed length.
   *
   * @return random byte array. When no payload should be generated, Optional.empty() is generated.
   *     Whether the payload should be generated is determined in construct time. "Number of
   *     records" and "execution time" are considered.
   */
  public Optional<byte[]> payload() {
    if (timeOut() || produced.getAndUpdate(previous -> Math.min(records, previous + 1)) >= records)
      return Optional.empty();
    byte[] payload = (this.fixedSize) ? new byte[size] : new byte[rand.nextInt(size) + 1];
    rand.nextBytes(payload);
    return Optional.of(payload);
  }

  public long end() {
    return this.end;
  }

  public long records() {
    return this.records;
  }

  public long produced() {
    return this.produced.get();
  }

  public void consumedIncrement() {
    this.consumed.incrementAndGet();
  }

  public long consumed() {
    return this.consumed.get();
  }

  public void countDownGetAssignment() {
    this.getAssignment.countDown();
  }

  public void awaitGetAssignment() throws InterruptedException {
    getAssignment.await();
  }

  /**
   * @return true on two conditions:
   *     <ol>
   *       <li>The consumers consumed the max number that producers would generate.
   *       <li>The consumers consumed **all the records** that producers produced, and the producers
   *           stop producing record (timeout).
   *     </ol>
   */
  public synchronized boolean consumedDone() {
    return consumed() == this.records || (timeOut() && consumed() >= produced());
  }

  private boolean timeOut() {
    return System.currentTimeMillis() >= end;
  }
}
