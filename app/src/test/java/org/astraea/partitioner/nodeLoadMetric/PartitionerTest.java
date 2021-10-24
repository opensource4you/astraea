package org.astraea.partitioner.nodeLoadMetric;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.astraea.partitioner.partitionerFactory.LinkPartitioner;
import org.astraea.service.RequireBrokerCluster;
import org.astraea.topic.TopicAdmin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PartitionerTest extends RequireBrokerCluster {
  private MBeanServer mBeanServer;
  private JMXConnectorServer jmxServer;
  public final String brokerList = bootstrapServers();
  TopicAdmin admin = TopicAdmin.of(bootstrapServers());
  public final String topicName = "address";

  @BeforeEach
  void setUp() throws IOException {
    //        JMXServiceURL serviceURL = new
    // JMXServiceURL(String.format("service:jmx:rmi://127.0.0.1:9999"));
    JMXServiceURL serviceURL = new JMXServiceURL("service:jmx:rmi://127.0.0.1");

    mBeanServer = ManagementFactory.getPlatformMBeanServer();

    jmxServer = JMXConnectorServerFactory.newJMXConnectorServer(serviceURL, null, mBeanServer);
    jmxServer.start();
    System.out.println(jmxServer.getAddress());
    System.out.println(brokerList);
  }

  @AfterEach
  void tearDown() throws Exception {
    jmxServer.stop();
    mBeanServer = null;
  }

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
    props.put("jmx_servers", "127.0.0.1@0");
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
        System.out.println("key:" + record1.key() + "***partition:" + recordMetadata.partition());
        System.out.println("key:" + record2.key() + "***partition:" + recordMetadata2.partition());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
