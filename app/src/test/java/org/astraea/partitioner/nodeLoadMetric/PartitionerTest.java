package org.astraea.partitioner.nodeLoadMetric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.astraea.partitioner.partitionerFactory.LinkPartitioner;
import org.astraea.service.RequireBrokerCluster;
import org.astraea.topic.TopicAdmin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionerTest extends RequireBrokerCluster {
  public final String brokerList = bootstrapServers();
  TopicAdmin admin = TopicAdmin.of(bootstrapServers());
  public final String topicName = "address";

  public Properties initConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, LinkPartitioner.class.getName());
    return props;
  }

  @Test
  public void testPartitioner() {
    Properties props = initConfig();
    props.put("jmx_servers", jmxServiceURL() + "@0");
    admin.createTopic(topicName, 10);
    KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    try {
      for (int i = 0; i < 10; i++) {
        ProducerRecord<String, String> record1 =
            new ProducerRecord<String, String>(topicName, "shanghai", Integer.toString(i));
        RecordMetadata recordMetadata = producer.send(record1).get();
        ProducerRecord<String, String> record2 =
            new ProducerRecord<String, String>(topicName, "tainan-" + i, Integer.toString(i));
        RecordMetadata recordMetadata2 = producer.send(record2).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    AdminClient client = AdminClient.create(props);
    Collection<Integer> brokerID = new ArrayList<>();
    brokerID.add(0);
    DescribeClusterResult broker = client.describeCluster();
    DescribeLogDirsResult result = client.describeLogDirs(brokerID);
    for (var k : result.descriptions().values()) {
      try {
        var map = k.get();
        for (String name : map.keySet()) {
          for (var p : map.get(name).replicaInfos().keySet()) {
            if (!p.topic().equals("__consumer_offsets")) {
              var size = (float) map.get(name).replicaInfos().get(p).size();
              Assertions.assertNotEquals(size, 0);
            }
          }
        }
      } catch (InterruptedException | ExecutionException e) {
        System.out.println("取得資訊失敗" + e.getMessage());
        e.printStackTrace();
      }
    }
  }
}
