package org.astraea.partitioner.partitionerFactory;

import java.io.IOException;
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

public class SmoothWeightPartitioner implements Partitioner {

  //  private static final BeanCollectorFactory FACTORY =
  //      new BeanCollectorFactory(
  //          Comparator.comparing(o -> o.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).toString()));

  //  private Map configs;

  private NodeLoadClient nodeLoadClient;

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    var overLoadCount = nodeLoadClient.nodesOverLoad(cluster);
    var loadPoisson = new LoadPoisson();
    var brokersWeight = new BrokersWeight();
    brokersWeight.setBrokerHashMap(loadPoisson.setAllPoisson(overLoadCount));
    Map.Entry<Integer, int[]> maxWeightServer = null;

    int allWeight = brokersWeight.getAllWeight();
    HashMap<Integer, int[]> currentBrokerHashMap = brokersWeight.getBrokerHashMap();

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
    brokersWeight.setCurrentBrokerHashMap(currentBrokerHashMap);

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
