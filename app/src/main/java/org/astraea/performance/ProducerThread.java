package org.astraea.performance;

import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.ProducerRecord;

/** 每秒傳送一則給定大小(recordSize)的訊息，直到已經傳送records筆訊息 */
public class ProducerThread extends CloseableThread {
  private final Producer producer;
  private final String topic;
  private final byte[] payload;
  private final long records;
  private final Metrics latency;

  /**
   * @param producer An instance of producer which can send record and update metrics
   * @param topic ProducerThread 會發送訊息到topic
   * @param records 要發送的訊息數量
   * @param recordSize 要發送訊息的大小
   * @param latency A metric that store statistic numbers.
   */
  public ProducerThread(
      Producer producer, String topic, long records, int recordSize, Metrics latency) {
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
  protected void execute() {
    for (int i = 0; i < records; ++i) {
      long start = System.currentTimeMillis();
      ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic, payload);
      try {
        producer.send(record).get();
        latency.putLatency(System.currentTimeMillis() - start);
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
  protected void cleanup() {
    producer.close();
  }
}
