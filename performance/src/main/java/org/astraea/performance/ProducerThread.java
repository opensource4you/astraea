package org.astraea.performance;

import java.util.Properties;

/*
 * 每秒傳送一則給定大小(recordSize)的訊息，直到已經傳送records筆訊息
 */
public class ProducerThread extends Thread {
  // private KafkaProducer<byte[], byte[]> producer;
  private String topic;
  private byte[] value;
  private long records;

  /*
   * @param topic     ProducerThread 會發送訊息到topic
   * @param bootstrapServers  kafka brokers 的位址
   * @param records   要發送的訊息數量
   */
  public ProducerThread(String topic, String bootstrapServers, long records, int recordSize) {
    // 設定producer.
    Properties prop = new Properties();
    prop.put("bootstrap.servers", bootstrapServers);
    prop.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    prop.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    // producer = KafkaProducer<byte[], byte[]>(prop);

    this.topic = topic;

    // 給定訊息大小
    value = new byte[recordSize];

    this.records = records;
  }

  /*
   * 執行(records)次資料傳送
   */
  @Override
  public void run() {
    //
  }
}
