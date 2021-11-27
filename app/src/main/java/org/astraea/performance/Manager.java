package org.astraea.performance;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/** Thread safe */
public class Manager {
  private final Performance.Argument.ExeTime exeTime;
  private final boolean fixedSize;
  private final int size;
  private final CountDownLatch getAssignment;
  private final CountDownLatch producerClosed;
  private final Random rand = new Random();

  private final List<Metrics> producerMetrics, consumerMetrics;
  private final long start = System.currentTimeMillis();

  /**
   * Used to manage producing/consuming.
   *
   * @param exeTime number of records at most to produce or execution time (includes initialization
   *     time)
   * @param fixedSize whether the length of the record should be random
   * @param size the length/(bound of random length) of the record
   */
  public Manager(
      Performance.Argument argument, List<Metrics> producerMetrics, List<Metrics> consumerMetrics) {
    this.fixedSize = argument.fixedSize;
    this.size = argument.recordSize;
    this.getAssignment = new CountDownLatch(argument.consumers);
    this.producerClosed = new CountDownLatch(argument.producers);
    this.producerMetrics = producerMetrics;
    this.consumerMetrics = consumerMetrics;
    this.exeTime = argument.exeTime;
  }

  /**
   * Generate random byte array in random/fixed length.
   *
   * @return random byte array. When no payload should be generated, Optional.empty() is generated.
   *     Whether the payload should be generated is determined in construct time. "Number of
   *     records" and "execution time" are considered.
   */
  public Optional<byte[]> payload() {
    if (producedDone()) return Optional.empty();
    byte[] payload = (this.fixedSize) ? new byte[size] : new byte[rand.nextInt(size) + 1];
    rand.nextBytes(payload);
    return Optional.of(payload);
  }

  public long producedRecords() {
    long ans = 0;
    for (var m : producerMetrics) ans += m.num();
    return ans;
  }

  public long consumedRecords() {
    long ans = 0;
    for (var m : consumerMetrics) ans += m.num();
    return ans;
  }

  public void countDownGetAssignment() {
    this.getAssignment.countDown();
  }

  public void countDownProducerClosed() {
    this.producerClosed.countDown();
  }

  public void awaitPartitionAssignment() throws InterruptedException {
    getAssignment.await();
  }

  public Performance.Argument.ExeTime exeTime() {
    return exeTime;
  }

  public boolean producedDone() {
    if (exeTime.mode == Performance.Argument.ExeTime.Mode.DURATION) {
      return System.currentTimeMillis() - start >= exeTime.duration.toMillis();
    } else {
      return producedRecords() >= exeTime.records;
    }
  }

  public boolean consumedDone() {
    return producedDone() && consumedRecords() >= producedRecords();
  }
}
