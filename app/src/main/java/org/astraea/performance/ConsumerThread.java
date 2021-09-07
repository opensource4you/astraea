package org.astraea.performance;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/*
 * 負責不斷消費訊息，並印出每筆資料的延時
 */
public class ConsumerThread extends Thread {
  // private KafkaConsumer<byte[], byte[]> consumer;
  private KafkaConsumer<String, byte[]> consumer;
  private boolean running;
  private long latency;

  public ConsumerThread(String topic, String bootstrapServers, String groupId) {
    Properties prop = new Properties();
    prop.put("bootstrap.servers", bootstrapServers);
    prop.put("group.id", groupId);
    prop.put("enable.auto.commit", "true");
    prop.put("auto.commit.interval.ms", "100");
    prop.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    prop.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    consumer = new KafkaConsumer<String, byte[]>(prop);
    consumer.subscribe(Arrays.asList(topic));

    running = true;
    latency = 0l;
  }

  @Override
  public void run() {
    while (running) {
      // 100毫秒內沒有取得資料，則重新檢查consumer是否該關閉。
      ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, byte[]> record : records) {
        // 取得端到端延時
        latency = System.currentTimeMillis() - record.timestamp();
        System.out.println("End to end latency: " + latency + " ms");
      }
    }
    consumer.close();
  }

  public void close() {
    running = false;
  }
}
