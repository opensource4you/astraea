package org.astraea.performance;

import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;

public interface TopicAdmin extends AutoCloseable {
  Set<String> listTopics() throws InterruptedException, ExecutionException;

  Map<String, KafkaFuture<Void>> createTopics(Collection<NewTopic> topics);

  List<TopicPartitionInfo> partitions(String topic);

  static TopicAdmin fromKafka(Properties prop) {
    Admin admin = Admin.create(prop);
    return new TopicAdmin() {
      @Override
      public Set<String> listTopics() throws InterruptedException, ExecutionException {
        return admin.listTopics().names().get();
      }

      @Override
      public Map<String, KafkaFuture<Void>> createTopics(Collection<NewTopic> topics) {
        return admin.createTopics(topics).values();
      }

      @Override
      public void close() {
        try {
          admin.close();
        } catch (Exception ignore) {
        }
      }

      @Override
      public List<TopicPartitionInfo> partitions(String topic) {
        try {
          return admin
              .describeTopics(Collections.singleton(topic))
              .values()
              .get(topic)
              .get()
              .partitions();
        } catch (Exception ignore) {
          return List.of();
        }
      }
    };
  }
}
