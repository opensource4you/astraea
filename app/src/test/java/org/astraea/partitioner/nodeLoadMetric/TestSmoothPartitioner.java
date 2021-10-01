package org.astraea.partitioner.nodeLoadMetric;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

import java.util.*;

import static java.util.Arrays.asList;
import static org.astraea.partitioner.nodeLoadMetric.NodeLoadClient.setOverLoadNode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSmoothPartitioner {

    private final static byte[] KEY_BYTES = "key".getBytes();
    private final static Node[] NODES = new Node[] {
            new Node(0, "localhost", 99),
            new Node(1, "localhost", 100),
            new Node(12, "localhost", 101)
    };
    private final static String TOPIC = "test";
    // Intentionally make the partition list not in partition order to test the edge cases.
    private final static List<PartitionInfo> PARTITIONS = asList(new PartitionInfo(TOPIC, 1, null, NODES, NODES),
            new PartitionInfo(TOPIC, 2, NODES[1], NODES, NODES),
            new PartitionInfo(TOPIC, 0, NODES[0], NODES, NODES));

    @Test
    public void testKeyPartitionIsStable() {
        final Partitioner partitioner = new SmoothPartitioner();
        final Cluster cluster = new Cluster("clusterId", asList(NODES), PARTITIONS,
                Collections.<String>emptySet(), Collections.<String>emptySet());
        int partition = partitioner.partition("test",  null, KEY_BYTES, null, null, cluster);
        assertEquals(partition, partitioner.partition("test", null, KEY_BYTES, null, null, cluster), "Same key should yield same partition");
    }
}
