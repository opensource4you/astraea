package org.astraea.performance;

import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.ProducerRecord;

/** 每秒傳送一則給定大小(recordSize)的訊息，直到已經傳送records筆訊息 */
public class ProducerThread extends CloseableThread {
  private Producer producer;
  private String topic;
  private byte[] payload;
  private long records;
  private AvgLatency latency;

  /**
   * @param topic ProducerThread 會發送訊息到topic
   * @param bootstrapServers kafka brokers 的位址
   * @param records 要發送的訊息數量
   * @param recordSize 要發送訊息的大小
   */
  public ProducerThread(
      Producer producer, String topic, long records, int recordSize, AvgLatency latency) {
    this.producer = producer;
    this.topic = topic;
    this.records = records;
    // 給定訊息大小
    payload = new byte[recordSize];
    // 記錄publish latency and bytes output
    this.latency = latency;
  }

  /** 執行(records)次資料傳送 */
  @Override
  public void execute() {
    for (int i = 0; i < records; ++i) {
      long start = System.currentTimeMillis();
      ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic, payload);
      try {
        producer.send(record).get();
        latency.put(System.currentTimeMillis() - start);
        latency.addBytes(payload.length);
        Thread.sleep(1);
      } catch (InterruptedException ie) {
      } catch (ExecutionException ee) {
      }
    }
    // execute once.
    closed.set(true);
  }

  @Override
  public void cleanup() {
    producer.close();
  }
}
