package org.astraea.partitioner.nodeLoadMetric;

import static org.astraea.partitioner.nodeLoadMetric.NodeLoadClient.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

public class SmoothPartitioner implements Partitioner {

  /** Implement Smooth Weight Round Robin. */
  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

    NodeLoadClient nodeLoadClient = null;
    try {
      nodeLoadClient = getNodeLoadInstance(cluster.nodes().size());
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    LoadPoisson loadPoisson = new LoadPoisson(nodeLoadClient);
    BrokersWeight brokersWeight = new BrokersWeight(loadPoisson);
    brokersWeight.setBrokerHashMap();
    Map.Entry<Integer, int[]> maxWeightServer = null;

    int allWeight = brokersWeight.getAllWeight();
    HashMap<Integer, int[]> currentBrokerHashMap = brokersWeight.getBrokerHashMap();

    for (Map.Entry<Integer, int[]> item : currentBrokerHashMap.entrySet()) {
      Map.Entry<Integer, int[]> currentServer = item;
      if (maxWeightServer == null || currentServer.getValue()[1] > maxWeightServer.getValue()[1]) {
        maxWeightServer = currentServer;
      }
    }
    assert maxWeightServer != null;
    currentBrokerHashMap.put(
        maxWeightServer.getKey(),
        new int[] {maxWeightServer.getValue()[0], maxWeightServer.getValue()[1] - allWeight});
    brokersWeight.setCurrentBrokerHashMap(currentBrokerHashMap);

    ArrayList<Integer> partitionList = new ArrayList<>();
    for (PartitionInfo partitionInfo : cluster.partitionsForNode(maxWeightServer.getKey())) {
      partitionList.add(partitionInfo.partition());
    }
    Random rand = new Random();
    nodeLoadClient.tellAlive();
    return partitionList.get(rand.nextInt(partitionList.size()));
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {}
}
