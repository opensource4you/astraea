package org.astraea.performance;

import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/** 負責不斷消費訊息，並記錄資料延時 */
public class ConsumerThread extends CloseableThread {
  private final Consumer consumer;
  private final AvgLatency latency;

  public ConsumerThread(Consumer consumer, AvgLatency latency) {
    this.consumer = consumer;
    this.latency = latency;
  }

  @Override
  public void execute() {
    // 100毫秒內沒有取得資料，則重新檢查consumer是否該關閉。
    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<byte[], byte[]> record : records) {
      // 取得端到端延時
      latency.put(System.currentTimeMillis() - record.timestamp());
      // 記錄輸入byte(沒有算入header和timestamp)
      latency.addBytes(record.serializedValueSize());
    }
  }

  @Override
  public void cleanup() {
    consumer.close();
  }
}
