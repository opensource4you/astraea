package org.astraea.performance;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/*
 * 每秒傳送一則給定大小(recordSize)的訊息，直到已經傳送records筆訊息
 */
public class ProducerThread extends Thread {
  private KafkaProducer<String, byte[]> producer;
  private String topic;
  private byte[] payload;
  private long records;
  private volatile boolean end;

  /*
   * @param topic     ProducerThread 會發送訊息到topic
   * @param bootstrapServers  kafka brokers 的位址
   * @param records   要發送的訊息數量
   */
  public ProducerThread(String topic, String bootstrapServers, long records, int recordSize) {
    // 設定producer
    Properties prop = new Properties();
    prop.put("bootstrap.servers", bootstrapServers);
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    prop.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producer = new KafkaProducer<String, byte[]>(prop);

    this.topic = topic;

    // 給定訊息大小
    payload = new byte[recordSize];

    this.records = records;

    this.end = false;
  }

  /*
   * 執行(records)次資料傳送
   */
  @Override
  public void run() {
    // 每秒傳送一筆訊息
    for (long i = 0; i < records; ++i) {
      producer.send(new ProducerRecord<String, byte[]>(topic, payload));
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
      }
    }
    System.out.println("Producer send end");
    this.end = true;
  }

  public boolean end() {
    return this.end;
  }
}
