package org.astraea.performance.latency;

import java.util.Objects;

class ConsumerThread extends CloseableThread {

  private final Consumer consumer;
  private final DataManager dataManager;
  private final MeterTracker tracker;

  ConsumerThread(DataManager dataManager, MeterTracker tracker, Consumer consumer) {
    this.dataManager = Objects.requireNonNull(dataManager);
    this.consumer = Objects.requireNonNull(consumer);
    this.tracker = Objects.requireNonNull(tracker);
  }

  @Override
  public void execute() throws InterruptedException {
    try {
      var now = System.currentTimeMillis();
      var records = consumer.poll();
      records.forEach(
          record -> {
            var entry = dataManager.removeSendingRecord(record.key());
            var latency = now - entry.getValue();
            var produceRecord = entry.getKey();
            if (!KafkaUtils.equal(produceRecord, record))
              System.out.println("receive corrupt data!!!");
            else tracker.record(record.serializedKeySize() + record.serializedValueSize(), latency);
          });
    } catch (org.apache.kafka.common.errors.WakeupException e) {
      throw new InterruptedException(e.getMessage());
    }
  }

  @Override
  void cleanup() {
    consumer.close();
  }

  @Override
  public void close() {
    consumer.wakeup();
    super.close();
  }
}
