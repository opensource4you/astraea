package org.astraea.performance;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

public interface TopicAdmin extends AutoCloseable {
  Set<String> listTopics() throws InterruptedException, ExecutionException;

  Map<String, KafkaFuture<Void>> createTopics(Collection<NewTopic> topics);

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
        } catch (Exception e) {
        }
      }
    };
  }
}
