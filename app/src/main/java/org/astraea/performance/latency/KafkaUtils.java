package org.astraea.performance.latency;

import java.util.*;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

final class KafkaUtils {

  static Header header(String key, byte[] value) {
    return new RecordHeader(key, value);
  }

  static boolean equal(Iterable<Header> lhs, Iterable<Header> rhs) {
    Comparator<Header> comparator =
        (o1, o2) -> {
          var r = o1.key().compareTo(o2.key());
          if (r != 0) return r;
          else return Arrays.compare(o1.value(), o2.value());
        };
    var lhsList = new ArrayList<Header>();
    lhs.forEach(lhsList::add);
    var rhsList = new ArrayList<Header>();
    rhs.forEach(rhsList::add);
    lhsList.sort(comparator);
    rhsList.sort(comparator);
    return lhsList.equals(rhsList);
  }

  static boolean equal(
      ProducerRecord<byte[], byte[]> producerRecord,
      ConsumerRecord<byte[], byte[]> consumerRecord) {
    if (!Arrays.equals(producerRecord.key(), consumerRecord.key())) return false;
    if (!Arrays.equals(producerRecord.value(), consumerRecord.value())) return false;
    if (!producerRecord.topic().equals(consumerRecord.topic())) return false;
    if (!equal(producerRecord.headers(), consumerRecord.headers())) return false;
    return producerRecord.timestamp() == null
        || producerRecord.timestamp() == consumerRecord.timestamp();
  }

  static void createTopicIfNotExist(
      TopicAdmin adminClient, Set<String> topics, int numberOfPartitions) {
    try {
      topics.forEach(
          topic -> {
            if (!adminClient.listTopics().contains(topic))
              adminClient.createTopics(
                  Collections.singletonList(
                      new NewTopic(topic, Optional.of(numberOfPartitions), Optional.empty())));
          });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private KafkaUtils() {}
}
