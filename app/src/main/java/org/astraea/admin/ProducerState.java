package org.astraea.admin;

import java.util.Objects;

public final class ProducerState {

  public static ProducerState from(org.apache.kafka.clients.admin.ProducerState state) {
    return new ProducerState(
        state.producerId(), state.producerEpoch(), state.lastSequence(), state.lastTimestamp());
  }

  private final long producerId;
  private final int producerEpoch;
  private final int lastSequence;
  private final long lastTimestamp;

  public ProducerState(long producerId, int producerEpoch, int lastSequence, long lastTimestamp) {
    this.producerId = producerId;
    this.producerEpoch = producerEpoch;
    this.lastSequence = lastSequence;
    this.lastTimestamp = lastTimestamp;
  }

  public long producerId() {
    return producerId;
  }

  public int producerEpoch() {
    return producerEpoch;
  }

  public int lastSequence() {
    return lastSequence;
  }

  public long lastTimestamp() {
    return lastTimestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ProducerState that = (ProducerState) o;
    return producerId == that.producerId
        && producerEpoch == that.producerEpoch
        && lastSequence == that.lastSequence
        && lastTimestamp == that.lastTimestamp;
  }

  @Override
  public int hashCode() {
    return Objects.hash(producerId, producerEpoch, lastSequence, lastTimestamp);
  }

  @Override
  public String toString() {
    return "ProducerState{"
        + "producerId="
        + producerId
        + ", producerEpoch="
        + producerEpoch
        + ", lastSequence="
        + lastSequence
        + ", lastTimestamp="
        + lastTimestamp
        + '}';
  }
}
