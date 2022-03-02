package org.astraea.performance;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.astraea.Utils;
import org.astraea.utils.DataSize;
import org.astraea.utils.DataUnit;

/**
 * Thread safe This class is used for managing the start/end of the producer/consumer threads.
 * Producer can start producing until all consumers get assignment. Consumers can stop after all
 * producers are closed and all records are consumed.
 */
public class Manager {
  private final ExeTime exeTime;
  private final CountDownLatch getAssignment;
  private final AtomicInteger producerClosed;

  private final List<Metrics> producerMetrics, consumerMetrics;
  private final long start = System.currentTimeMillis();
  private final AtomicLong payloadNum = new AtomicLong(0);
  private final Distribution distribution;
  private final RandomContent randomContent;
  private long intervalStart = System.currentTimeMillis();
  private long payloadBytes = 0L;
  private final DataSize throughput;

  /**
   * Used to manage producing/consuming.
   *
   * @param argument
   *     <ol>
   *       <li>"fixedSize" for setting whether the size of the record is fixed
   *       <li>"recordSize" for setting (the bound of) the size of the record
   *       <li>"consumers" for number of consumers to wait on getting assignment
   *       <li>"producers" for number of producers to wait on stop producing
   *       <li>"exeTime" for determining whether the producers/consumers are completed
   *     </ol>
   *
   * @param producerMetrics for counting the number of records have been produced
   * @param consumerMetrics for counting the number of records have been consumed
   */
  public Manager(
      Performance.Argument argument, List<Metrics> producerMetrics, List<Metrics> consumerMetrics) {
    if (argument.recordSize.greaterThan(DataUnit.Byte.of(Integer.MAX_VALUE)))
      throw new IllegalArgumentException(
          "Record size should be smaller than or equal to 2147483648 (Integer.MAX_VALUE) bytes");
    this.getAssignment = new CountDownLatch(argument.consumers);
    this.producerClosed = new AtomicInteger(argument.producers);
    this.producerMetrics = producerMetrics;
    this.consumerMetrics = consumerMetrics;
    this.exeTime = argument.exeTime;
    this.distribution = argument.distribution;
    this.randomContent = new RandomContent(argument.recordSize, argument.fixedSize);
    this.throughput = argument.throughput;
  }

  /**
   * Generate random byte array in random/fixed length. Warning: This method will block when the
   * throughput (content generating rate) is higher than the given number (argument.throughput).
   *
   * @return random byte array. When no payload should be generated, Optional.empty() is generated.
   *     Whether the payload should be generated is determined in construct time. "Number of
   *     records" and "execution time" are considered.
   */
  public Optional<byte[]> payload() {
    if (exeTime.percentage(payloadNum.getAndIncrement(), System.currentTimeMillis() - start)
        >= 100D) return Optional.empty();

    var payload = randomContent.getContent();

    Utils.waitFor(this::notThrottled);
    payloadBytes += payload.length;
    return Optional.of(payload);
  }

  synchronized boolean notThrottled() {
    if (System.currentTimeMillis() - intervalStart > 1000) {
      intervalStart = System.currentTimeMillis();
      payloadBytes = 0L;
    }
    return payloadBytes < throughput.measurement(DataUnit.Byte).intValue();
  }

  public long producedRecords() {
    return producerMetrics.stream().mapToLong(Metrics::num).sum();
  }

  public long consumedRecords() {
    return consumerMetrics.stream().mapToLong(Metrics::num).sum();
  }

  /** Used in "consumerRebalanceListener" callback. To */
  public void countDownGetAssignment() {
    this.getAssignment.countDown();
  }

  /** Called after producer is closed. For informing consumers there will be no new records */
  public void producerClosed() {
    this.producerClosed.decrementAndGet();
  }

  public void awaitPartitionAssignment() throws InterruptedException {
    getAssignment.await();
  }

  public ExeTime exeTime() {
    return exeTime;
  }

  /** Check if all producer are closed. */
  public boolean producedDone() {
    return producerClosed.get() == 0;
  }

  /** Check if we should keep consuming record. */
  public boolean consumedDone() {
    return producedDone()
        && (consumerMetrics.size() == 0 || consumedRecords() >= producedRecords());
  }

  /** Randomly choose a key according to the distribution. */
  public byte[] getKey() {
    return (String.valueOf(distribution.get())).getBytes();
  }

  /** Randomly generate content before {@link #getContent()} is called. */
  private static class RandomContent {
    private final Random rand = new Random();
    private final DataSize dataSize;
    private final boolean fixedSize;
    private final byte[] content;

    /**
     * @param dataSize The size of each random generated content in bytes.
     * @param fixedSize Determine whether to fix the size of random generated content
     */
    public RandomContent(DataSize dataSize, boolean fixedSize) {
      this.dataSize = dataSize;
      this.fixedSize = fixedSize;
      content = new byte[dataSize.measurement(DataUnit.Byte).intValue()];
    }

    public byte[] getContent() {
      // Randomly change one position of the content;
      content[rand.nextInt(dataSize.measurement(DataUnit.Byte).intValue())] =
          (byte) rand.nextInt(256);
      if (fixedSize) return Arrays.copyOf(content, content.length);
      else return Arrays.copyOfRange(content, rand.nextInt(content.length), content.length);
    }
  }
}
