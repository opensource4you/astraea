package org.astraea.partitioner.nodeLoadMetric;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
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

  public Properties initProConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "id1");
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, LinkPartitioner.class.getName());
    return props;
  }

  public Properties initConConfig() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "id1");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put("group.id", "group1");
    return props;
  }

  @Test
  public void testPartitioner() {
    Properties props = initProConfig();
    props.put("jmx_servers", jmxServiceURL() + "@0");
    admin.createTopic(topicName, 10);
    KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    try {
      for (int i = 0; i < 20; i++) {
        ProducerRecord<String, String> record2 =
            new ProducerRecord<String, String>(topicName, "tainan-" + i, Integer.toString(i));
        RecordMetadata recordMetadata2 = producer.send(record2).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    KafkaConsumer<String, String> consumer = new KafkaConsumer(initConConfig());
    consumer.subscribe(Arrays.asList(topicName));
    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(10));
    if (!consumerRecords.isEmpty()) {
      var count = 0;
      for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
        if (Pattern.compile("^tainan").matcher(consumerRecord.key()).find()) count++;
        //        System.out.println("TopicName: " + consumerRecord.topic() + " Partition:" +
        //                consumerRecord.partition() + " Offset:" + consumerRecord.offset() + "" +
        //                " Msg:" + consumerRecord.value());
      }
      Assertions.assertEquals(count, 20);
    }

    AdminClient client = AdminClient.create(props);
    Collection<Integer> brokerID = new ArrayList<>();
    brokerID.add(0);

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
