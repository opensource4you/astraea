package org.astraea.performance;

import java.util.Collection;
import java.util.Properties;

public interface ComponentFactory {
  Producer createProducer();

  Consumer createConsumer(Collection<String> topic);

  TopicAdmin createAdmin();

  static ComponentFactory fromKafka(String brokers) {
    return new ComponentFactory() {
      private final String groupId = "groupId:" + System.currentTimeMillis();
      /** Create Producer with KafkaProducer<byte[], byte[]> functions */
      @Override
      public Producer createProducer() {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", brokers);
        return Producer.fromKafka(prop);
      }

      /** Create Consumer with KafkaConsumer<byte[], byte[]> functions */
      @Override
      public Consumer createConsumer(Collection<String> topic) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", brokers);
        prop.put("group.id", groupId);
        return Consumer.fromKafka(prop, topic);
      }

      /** Create TopicAdmin with KafkaAdminClient functions */
      @Override
      public TopicAdmin createAdmin() {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", brokers);
        return TopicAdmin.fromKafka(prop);
      }
    };
  }
}
