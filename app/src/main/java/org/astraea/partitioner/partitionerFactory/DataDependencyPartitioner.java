package org.astraea.partitioner.partitionerFactory;

import static org.astraea.partitioner.partitionerFactory.LinkPartitioner.getAddressMap;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.astraea.partitioner.nodeLoadMetric.NodeLoadClient;
import org.astraea.partitioner.nodeLoadMetric.NodeMetadata;

public class DataDependencyPartitioner implements Partitioner {
  private NodeLoadClient nodeLoadClient;
  private String minNodeID;
  private int partitionID = -1;

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    if (partitionID < 0) {
      var partitions =
          cluster.partitionsForNode(Integer.parseInt(minNodeID)).stream()
              .map(PartitionInfo::partition)
              .collect(Collectors.toList());
      Random rand = new Random();
      partitionID = partitions.get(rand.nextInt(partitions.size()));
    }
    return partitionID;
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {
    try {
      nodeLoadClient = new NodeLoadClient((getAddressMap(configs)));
      nodeLoadClient.refreshNodesMetrics();
      minNodeID =
          nodeLoadClient.getNodeMetadataCollection().stream()
              .min(Comparator.comparing(NodeMetadata::getTotalBytes))
              .get()
              .getNodeID();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
