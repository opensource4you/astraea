package org.astraea.performance;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread safe This class is used for managing the start/end of the producer/consumer threads.
 * Producer can start producing until all consumers get assignment. Consumers can stop after all
 * producers are closed and all records are consumed.
 */
public class Manager {
  private final ExeTime exeTime;
  private final boolean fixedSize;
  private final int size;
  private final CountDownLatch getAssignment;
  private final AtomicInteger producerClosed;
  private final Random rand = new Random();

  private final List<Metrics> producerMetrics, consumerMetrics;
  private final long start = System.currentTimeMillis();
  private final AtomicLong payloadNum = new AtomicLong(0);
  private final List<Map.Entry<Double, String>> keyPosTable = new ArrayList<>();

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
    this.fixedSize = argument.fixedSize;
    this.size = argument.recordSize;
    this.getAssignment = new CountDownLatch(argument.consumers);
    this.producerClosed = new AtomicInteger(argument.producers);
    this.producerMetrics = producerMetrics;
    this.consumerMetrics = consumerMetrics;
    this.exeTime = argument.exeTime;
    argument.distribution.forEach(
        s -> keyPosTable.add(Map.entry(Double.parseDouble(s.split(":")[1]), s.split(":")[0])));
    // Argument verification
    keyPosTable.forEach(
        entry -> {
          if (entry.getValue().getBytes().length > this.size)
            throw new IllegalArgumentException("One of the key is too big to fit in a record.");
        });
  }

  /**
   * Generate random byte array in random/fixed length.
   *
   * @return random byte array. When no payload should be generated, Optional.empty() is generated.
   *     Whether the payload should be generated is determined in construct time. "Number of
   *     records" and "execution time" are considered.
   */
  public Optional<byte[]> payload() {
    if (exeTime.percentage(payloadNum.getAndIncrement(), System.currentTimeMillis() - start)
        >= 100D) return Optional.empty();

    byte[] payload = (this.fixedSize) ? new byte[size] : new byte[rand.nextInt(size) + 1];
    rand.nextBytes(payload);
    return Optional.of(payload);
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
    return producedDone() && consumedRecords() >= producedRecords();
  }

  /** Randomly choose a key according to the possibility table. */
  public Optional<byte[]> getKey() {
    double r = rand.nextDouble();
    for (var keyPos : keyPosTable) {
      r -= keyPos.getKey();
      if (r <= 0) return Optional.of(keyPos.getValue().getBytes());
    }
    return Optional.empty();
  }
}
