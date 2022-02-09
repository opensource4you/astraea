package org.astraea.partitioner.dependency;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.partitioner.smoothPartitioner.SmoothWeightPartitioner;
import org.astraea.producer.Producer;
import org.astraea.producer.Serializer;
import org.astraea.service.RequireBrokerCluster;
import org.astraea.topic.TopicAdmin;
import org.junit.jupiter.api.Test;

public class DependencyRecordTest extends RequireBrokerCluster {
  private final String brokerList = bootstrapServers();
  private final String topicName = "address";
  TopicAdmin admin = TopicAdmin.of(bootstrapServers());

  private Properties initProConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "id1");
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SmoothWeightPartitioner.class.getName());
    props.put("producerID", 1);
    props.put("jmx_servers", jmxServiceURL().getHost() + ":" + jmxServiceURL().getPort());
    return props;
  }

  @Test
  void testDependencyRecord() {
    admin.creator().topic(topicName).numberOfPartitions(10).create();
    try (var producer =
        Producer.builder()
            .keySerializer(Serializer.STRING)
            .configs(
                initProConfig().entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)))
            .build()) {
      DependencyRecord<String, byte[]> dependencyRecord =
          new DependencyRecord(producer.kafkaProducer(), initProConfig(), topicName);
      dependencyRecord.partition();
    }
  }
}
