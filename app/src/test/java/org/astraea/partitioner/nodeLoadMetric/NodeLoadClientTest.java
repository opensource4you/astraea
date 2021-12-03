package org.astraea.partitioner.nodeLoadMetric;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.partitioner.partitionerFactory.SmoothWeightPartitioner;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NodeLoadClientTest extends RequireBrokerCluster {
  private final String brokerList = bootstrapServers();
  private final String topicName = "address";
  private String jmxAddress;

  @BeforeEach
  public void setUp() {}

  public Properties initProConfig() {
    Properties props = new Properties();
    jmxAddress = jmxServiceURL().getHost() + ":" + jmxServiceURL().getPort();
    System.out.println(jmxAddress);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "id1");
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SmoothWeightPartitioner.class.getName());
    props.put("jmx_servers", jmxAddress);
    return props;
  }

  @Test
  public void fetchMetrics() throws IOException {
    Map props = initProConfig();
    var map = new HashMap<String, String>();
    map.put(jmxServiceURL().getHost(), jmxServiceURL().getPort() + "");
    var nodeLoadClient = new NodeLoadClient(map, props);
    System.out.println(nodeLoadClient.beanCollector().objects());
    var FACTORY = nodeLoadClient.getFactory();
    var beanCollector = FACTORY.getOrCreate(props);
    System.out.println(beanCollector.objects());
    System.out.println("node" + beanCollector.nodes());
  }
}
