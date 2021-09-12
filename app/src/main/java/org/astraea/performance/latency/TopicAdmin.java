package org.astraea.performance.latency;

import java.io.Closeable;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;

interface TopicAdmin extends Closeable {

  static TopicAdmin fromKafka(Properties props) {
    var adminClient = Admin.create(props);

    return new TopicAdmin() {

      @Override
      public Set<String> listTopics() {
        try {
          return adminClient.listTopics().names().get();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void createTopics(Collection<NewTopic> newTopics) {
        try {
          adminClient.createTopics(newTopics).all().get();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void close() {
        adminClient.close();
      }
    };
  }

  /** see {@link Admin#listTopics()} */
  Set<String> listTopics();

  /** see {@link Admin#createTopics(Collection)} */
  void createTopics(Collection<NewTopic> newTopics);

  @Override
  default void close() {}
}
