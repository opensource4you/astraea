package org.astraea.partitioner.nodeLoadMetric;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

public class TestSmoothPartitioner {
  private HashMap<String, String> jmxAddresses;
  private Collection<NodeMetadata> nodeMetadataCollection;

  private static final byte[] KEY_BYTES = "key".getBytes();
  private static final Node[] NODES =
      new Node[] {
        new Node(0, "localhost", 99), new Node(1, "localhost", 100), new Node(12, "localhost", 101)
      };
  private static final String TOPIC = "test";
  // Intentionally make the partition list not in partition order to test the edge cases.
  private static final List<PartitionInfo> PARTITIONS =
      asList(
          new PartitionInfo(TOPIC, 1, null, NODES, NODES),
          new PartitionInfo(TOPIC, 2, NODES[1], NODES, NODES),
          new PartitionInfo(TOPIC, 0, NODES[0], NODES, NODES));

  @BeforeEach
  public void setUp() {
    jmxAddresses = new HashMap<>();
    jmxAddresses.put("0", "0.0.0.0");
    jmxAddresses.put("1", "0.0.0.0");
    jmxAddresses.put("2", "0.0.0.0");

    nodeMetadataCollection = new ArrayList<>();
    NodeMetrics nodeMetrics = mock(NodeMetrics.class);
    NodeMetadata nodeMetadata0 = new NodeMetadata("0", nodeMetrics);
    NodeMetadata nodeMetadata1 = new NodeMetadata("1", nodeMetrics);
    NodeMetadata nodeMetadata2 = new NodeMetadata("2", nodeMetrics);
    NodeMetadata nodeMetadata3 = new NodeMetadata("3", nodeMetrics);

    nodeMetadataCollection.add(nodeMetadata0);
    nodeMetadataCollection.add(nodeMetadata1);
    nodeMetadataCollection.add(nodeMetadata2);
    nodeMetadataCollection.add(nodeMetadata3);
  }

  @Test
  public void testKeyPartitionIsStable() {
    HashMap<String, Double> testBrokerMsg = new HashMap<>();
    testBrokerMsg.put("0", 500.0);
    testBrokerMsg.put("1", 1500.0);
    testBrokerMsg.put("2", 800.0);
    testBrokerMsg.put("3", 1200.0);
    Partitioner partitioner = new SmoothPartitioner();
    partitioner.configure(jmxAddresses);

    final Cluster cluster =
        new Cluster(
            "clusterId",
            asList(NODES),
            PARTITIONS,
            Collections.<String>emptySet(),
            Collections.<String>emptySet());
    try (MockedConstruction mocked = mockConstruction(NodeMetrics.class)) {
      int partition = partitioner.partition("test", null, KEY_BYTES, null, null, cluster);
      assertEquals(
          partition,
          partitioner.partition("test", null, KEY_BYTES, null, null, cluster),
          "Same key should yield same partition");
    }
  }
}
