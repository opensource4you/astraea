package org.astraea.performance;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/** Thread safe */
public class DataManager {
  private final long records;
  private final boolean fixedSize;
  private final int size;
  private final Random rand = new Random();

  // Random payload produced by this instance.
  private long produced = 0L;
  // Record the records consumed by consumer.
  private final AtomicLong consumed = new AtomicLong(0L);

  public DataManager(long records, boolean fixedSize, int size) {
    this.records = records;
    this.fixedSize = fixedSize;
    this.size = size;
  }

  /** Generate random byte array in random/fixed length. */
  public synchronized Optional<byte[]> payload() {
    if (produced >= records) return Optional.empty();
    ++produced;
    byte[] payload = (this.fixedSize) ? new byte[size] : new byte[rand.nextInt(size) + 1];
    rand.nextBytes(payload);
    return Optional.of(payload);
  }

  public long records() {
    return this.records;
  }

  public synchronized long produced() {
    return this.produced;
  }

  public synchronized void consumedIncrement() {
    this.consumed.incrementAndGet();
  }

  public synchronized long consumed() {
    return this.consumed.get();
  }
}
