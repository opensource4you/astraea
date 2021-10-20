package org.astraea.performance.latency;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.astraea.topic.TopicAdmin;

interface ComponentFactory {

  /**
   * create a factory based on kafka cluster. All components created by this factory will send
   * request to kafka to get responses.
   *
   * @param props kafka props
   * @param topics to subscribe
   * @return a factory based on kafka
   */
  static ComponentFactory fromKafka(Map<String, Object> props, Set<String> topics) {

    return new ComponentFactory() {
      private final String groupId = "group-id-" + System.currentTimeMillis();

      @Override
      public Producer producer() {
        return Producer.fromKafka(props);
      }

      @Override
      public Consumer consumer() {
        var copy = new HashMap<>(props);
        // all consumers are in same group, so there is no duplicate data in read workload.
        copy.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return Consumer.fromKafka(copy, topics);
      }

      @Override
      public TopicAdmin topicAdmin() {
        return TopicAdmin.of(props);
      }
    };
  }

  /** @return a new producer. Please close it when you don't need it. */
  Producer producer();

  /** @return a new consumer. Please close it when you don't need it */
  Consumer consumer();

  /** @return a new topic admin. Please close it when you don't need it. */
  TopicAdmin topicAdmin();
}
