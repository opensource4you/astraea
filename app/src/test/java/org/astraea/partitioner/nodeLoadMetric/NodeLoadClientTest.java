package org.astraea.partitioner.nodeLoadMetric;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.partitioner.partitionerFactory.SmoothWeightPartitioner;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class NodeLoadClientTest extends RequireBrokerCluster {
  private final String brokerList = bootstrapServers();
  private NodeLoadClient nodeLoadClient;
  private String jmxAddress;
  private Map props;

  @BeforeAll
  public void setUp() throws IOException {
    props = initProConfig();
    var map = new HashMap<String, Integer>();
    map.put(jmxServiceURL().getHost(), jmxServiceURL().getPort());
    nodeLoadClient = new NodeLoadClient(map);
  }

  @AfterAll
  public void tearDown() {
    nodeLoadClient.close();
  }

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
  public void testAvgBrokersMsgPerSec() {
    var testMap = new HashMap<Integer, List<Double>>();
    testMap.put(0, Arrays.asList(20.0, 30.0, 40.0));
    testMap.put(1, Arrays.asList(30.0, 20.0, 30.0));
    testMap.put(2, Arrays.asList(40.0, 40.0, 20.0));
    Assertions.assertEquals(
        nodeLoadClient.avgBrokersMsgPerSec(testMap), Arrays.asList(30.0, 30.0, 30.0));
  }

  @Test
  public void testStandardDeviationImperative() {
    var testMap = new HashMap<Integer, Double>();
    testMap.put(0, 15.0);
    testMap.put(1, 20.0);
    testMap.put(2, 25.0);
    testMap.put(3, 20.0);
    testMap.put(4, 20.0);
    Assertions.assertEquals(
        nodeLoadClient.standardDeviationImperative(testMap, 20), 3.1622776601683795);
  }
}
