package org.astraea.performance;

import java.util.Collection;
import java.util.Properties;

/** An interface used for creating producer, consumer, admin. */
public interface ComponentFactory {
  Producer createProducer();

  Consumer createConsumer(Collection<String> topic);

  TopicAdmin createAdmin();

  /**
   * Used for creating Kafka producer, consumer, admin of the same Kafka server. The consumers
   * generated by the same object from `fromKafka(brokers)` have the same groupID
   */
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
