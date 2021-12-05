package org.astraea.partitioner.partitionerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.astraea.partitioner.nodeLoadMetric.BrokersWeight;
import org.astraea.partitioner.nodeLoadMetric.LoadPoisson;
import org.astraea.partitioner.nodeLoadMetric.NodeLoadClient;

/**
 * Based on the jmx metrics obtained from Kafka, it records the load status of the node over a
 * period of time. Predict the future status of each node through the poisson of the load status.
 * Finally, the result of poisson is used as the weight to perform smooth weighted RoundRobin.
 */
public class SmoothWeightPartitioner implements Partitioner {

  private NodeLoadClient nodeLoadClient;

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    Map<Integer, Integer> overLoadCount = null;
    try {
      overLoadCount = nodeLoadClient.nodesOverLoad(cluster);
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    var loadPoisson = new LoadPoisson();
    var brokersWeight = new BrokersWeight();
    brokersWeight.brokerHashMap(loadPoisson.allPoisson(overLoadCount));
    Map.Entry<Integer, int[]> maxWeightServer = null;

    var allWeight = brokersWeight.allNodesWeight();
    var currentBrokerHashMap = brokersWeight.brokerHashMap();

    for (Map.Entry<Integer, int[]> item : currentBrokerHashMap.entrySet()) {
      if (maxWeightServer == null || item.getValue()[1] > maxWeightServer.getValue()[1]) {
        maxWeightServer = item;
      }
      currentBrokerHashMap.put(
          item.getKey(), new int[] {item.getValue()[0], item.getValue()[1] + item.getValue()[0]});
    }
    assert maxWeightServer != null;
    currentBrokerHashMap.put(
        maxWeightServer.getKey(),
        new int[] {maxWeightServer.getValue()[0], maxWeightServer.getValue()[1] - allWeight});
    brokersWeight.currentBrokerHashMap(currentBrokerHashMap);

    ArrayList<Integer> partitionList = new ArrayList<>();
    for (PartitionInfo partitionInfo : cluster.partitionsForNode(maxWeightServer.getKey())) {
      partitionList.add(partitionInfo.partition());
    }
    Random rand = new Random();

    return partitionList.get(rand.nextInt(partitionList.size()));
  }

  @Override
  public void close() {
    nodeLoadClient.close();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    try {
      var jmxAddresses =
          Objects.requireNonNull(
              configs.get("jmx_servers").toString(), "You must configure jmx_servers correctly");
      var list = Arrays.asList((jmxAddresses).split(","));
      HashMap<String, String> mapAddress = new HashMap<>();
      for (String str : list) {
        var listAddress = Arrays.asList(str.split(":"));
        mapAddress.put(listAddress.get(0), listAddress.get(1));
      }
      Objects.requireNonNull(mapAddress, "You must configure jmx_servers correctly.");

      nodeLoadClient = new NodeLoadClient(mapAddress, configs);
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }
}
