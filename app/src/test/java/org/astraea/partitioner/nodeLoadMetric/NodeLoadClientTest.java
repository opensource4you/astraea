package org.astraea.partitioner.nodeLoadMetric;

import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.astraea.Utils;
import org.astraea.partitioner.ClusterInfo;
import org.astraea.partitioner.NodeInfo;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class NodeLoadClientTest extends RequireBrokerCluster {
  private NodeLoadClient nodeLoadClient;

  @BeforeAll
  void setUp() {
    var map = new HashMap<Integer, Integer>();
    map.put(0, jmxServiceURL().getPort());
    nodeLoadClient = new NodeLoadClient(map, jmxServiceURL().getPort());
  }

  @AfterAll
  void tearDown() {
    nodeLoadClient.close();
  }

  private Field field(Object object, String fieldName) {
    Field field = null;
    try {
      field = object.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    }
    return field;
  }

  @Test
  void testBrokerLoad() {
    List<NodeLoadClient.Broker> brokers = new ArrayList<>();
    brokers.add(new NodeLoadClient.Broker(0, "0.0.0.0", 111));
    brokers.add(new NodeLoadClient.Broker(1, "0.0.0.0", 222));
    brokers.add(new NodeLoadClient.Broker(2, "0.0.0.0", 333));
    setBrokers(brokers);
    Assertions.assertEquals(nodeLoadClient.brokerLoad(0.37, 1.0 / 3), 1);
    Assertions.assertEquals(nodeLoadClient.brokerLoad(0.01, 1.0 / 3), 0);
    Assertions.assertEquals(nodeLoadClient.brokerLoad(0.8, 1.0 / 3), 2);
  }

  @Test
  void testStandardDeviationImperative() {
    var testMap = new HashMap<Integer, Double>();
    testMap.put(0, 15.0);
    testMap.put(1, 20.0);
    testMap.put(2, 25.0);
    testMap.put(3, 20.0);
    testMap.put(4, 20.0);

    List<NodeLoadClient.Broker> brokers = new ArrayList<>();
    brokers.add(0, setSituationNormalized(0, "0.0.0.0", 111, 15.0));
    brokers.add(1, setSituationNormalized(0, "0.0.0.0", 222, 20.0));
    brokers.add(2, setSituationNormalized(0, "0.0.0.0", 333, 25.0));
    brokers.add(3, setSituationNormalized(0, "0.0.0.0", 444, 20.0));
    brokers.add(4, setSituationNormalized(0, "0.0.0.0", 555, 20.0));
    setBrokers(brokers);

    Assertions.assertEquals(nodeLoadClient.standardDeviationImperative(20), 3.1622776601683795);
  }

  @Test
  void testLoadSituation() {
    var bootstrapServers = List.of(bootstrapServers().split(","));
    List<NodeInfo> nodes = new ArrayList<>();
    AtomicInteger count = new AtomicInteger(0);
    bootstrapServers.forEach(
        str -> {
          var hostPort = str.split(":");
          nodes.add(NodeInfo.of(count.get(), hostPort[0], Integer.parseInt(hostPort[1])));
          count.getAndIncrement();
        });
    var cluster = Mockito.mock(ClusterInfo.class);
    when(cluster.nodes()).thenReturn(nodes);
    var load = nodeLoadClient.loadSituation(cluster);
    Assertions.assertEquals(load.get(0), 1);
    Assertions.assertEquals(load.get(1), 1);
    Assertions.assertEquals(load.get(2), 1);
    load = nodeLoadClient.loadSituation(cluster);
    Assertions.assertEquals(load.get(0), 1);
    Assertions.assertEquals(load.get(1), 1);
    Assertions.assertEquals(load.get(2), 1);
    var lastTime = System.currentTimeMillis();
    Utils.waitFor(() -> Utils.overSecond(lastTime, 1));
    load = nodeLoadClient.loadSituation(cluster);
    Assertions.assertEquals(load.get(0), 2);
    Assertions.assertEquals(load.get(1), 2);
    Assertions.assertEquals(load.get(2), 2);
  }

  private NodeLoadClient.Broker setSituationNormalized(
      int brokerID, String host, int port, double situation) {
    var broker = new NodeLoadClient.Broker(brokerID, host, port);
    var brokerSituation = field(broker, "brokerSituationNormalized");
    try {
      brokerSituation.set(broker, situation);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return broker;
  }

  private void setBrokers(List<NodeLoadClient.Broker> brokers) {
    var brokersField = field(nodeLoadClient, "brokers");
    try {
      brokersField.set(nodeLoadClient, brokers);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }
}
