package org.astraea.performance.latency;

import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

interface ComponentFactory {

  /**
   * create a factory based on kafka cluster. All components created by this factory will send
   * request to kafka to get responses.
   *
   * @param brokers kafka broker addresses
   * @param topics to subscribe
   * @return a factory based on kafka
   */
  static ComponentFactory fromKafka(String brokers, Set<String> topics) {

    return new ComponentFactory() {
      private final String groupId = "group-id-" + System.currentTimeMillis();

      @Override
      public Producer producer() {
        var props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        return Producer.fromKafka(props);
      }

      @Override
      public Consumer createConsumer() {
        var props = new Properties();
        props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
        // all consumers are in same group, so there is no duplicate data in read workload.
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return Consumer.fromKafka(props, topics);
      }

      @Override
      public TopicAdmin createTopicAdmin() {
        var props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        return TopicAdmin.fromKafka(props);
      }
    };
  }

  /** @return a new producer. Please close it when you don't need it. */
  Producer producer();

  /** @return a new consumer. Please close it when you don't need it */
  Consumer createConsumer();

  /** @return a new topic admin. Please close it when you don't need it. */
  TopicAdmin createTopicAdmin();
}
