package org.astraea.performance.latency;

import java.time.Duration;
import java.util.Objects;

class ProducerThread extends CloseableThread {
  private final Producer producer;
  private final DataManager dataManager;
  private final MeterTracker tracker;
  private final Duration flushDuration;
  private long lastSend = 0;

  ProducerThread(
      DataManager dataManager, MeterTracker tracker, Producer producer, Duration flushDuration) {
    this.dataManager = Objects.requireNonNull(dataManager);
    this.producer = Objects.requireNonNull(producer);
    this.tracker = Objects.requireNonNull(tracker);
    this.flushDuration = Objects.requireNonNull(flushDuration);
  }

  @Override
  void execute() {
    var data = dataManager.producerRecord();
    var now = System.currentTimeMillis();
    dataManager.sendingRecord(data, now);
    producer
        .send(data)
        .whenComplete(
            (r, e) -> {
              if (e != null) tracker.record(0, System.currentTimeMillis() - now);
              else
                tracker.record(
                    r.serializedKeySize() + r.serializedValueSize(),
                    System.currentTimeMillis() - now);
            });
    if (lastSend <= 0) lastSend = now;
    else if (lastSend + flushDuration.toMillis() < now) {
      lastSend = now;
      producer.flush();
    }
  }

  @Override
  void cleanup() {
    producer.close();
  }
}
