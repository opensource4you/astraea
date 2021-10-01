package org.astraea.partitioner.nodeLoadMetric;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.*;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

public class TestSmoothPartitioner {

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

  @Test
  public void testKeyPartitionIsStable() {
    final Partitioner partitioner = new SmoothPartitioner();
    final Cluster cluster =
        new Cluster(
            "clusterId",
            asList(NODES),
            PARTITIONS,
            Collections.<String>emptySet(),
            Collections.<String>emptySet());
    int partition = partitioner.partition("test", null, KEY_BYTES, null, null, cluster);
    assertEquals(
        partition,
        partitioner.partition("test", null, KEY_BYTES, null, null, cluster),
        "Same key should yield same partition");
  }
}
