package org.astraea.performance;

import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

public interface TopicAdmin extends AutoCloseable {
  Set<String> listTopics() throws InterruptedException, ExecutionException;

  CreateTopicsResult createTopics(Collection<NewTopic> topics);

  static TopicAdmin fromKafka(Properties prop) {
    Admin admin = Admin.create(prop);
    return new TopicAdmin() {
      @Override
      public Set<String> listTopics() throws InterruptedException, ExecutionException {
        return admin.listTopics().names().get();
      }

      @Override
      public CreateTopicsResult createTopics(Collection<NewTopic> topics) {
        return admin.createTopics(topics);
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
