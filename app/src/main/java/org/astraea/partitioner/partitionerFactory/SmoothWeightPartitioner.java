package org.astraea.partitioner.partitionerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.astraea.partitioner.nodeLoadMetric.NodeLoadClient;
import org.astraea.partitioner.nodeLoadMetric.NodeLoadClientFactory;

public class SmoothWeightPartitioner implements Partitioner {

  private static final NodeLoadClientFactory FACTORY =
      new NodeLoadClientFactory(
          Comparator.comparing(o -> o.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).toString()));
  /**
   * Record the current weight of each node according to Poisson calculation and the weight after
   * partitioner calculation.
   */
  private HashMap<String, int[]> allBrokersWeight = new HashMap<>();

  private NodeLoadClient nodeLoadClient;

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    Map.Entry<String, int[]> maxWeightServer = null;
    updateBrokersWeight();
    var allWeight = getAllWeight();
    HashMap<String, int[]> currentBrokers = getAllBrokersWeight();

    for (Map.Entry<String, int[]> item : currentBrokers.entrySet()) {
      if (maxWeightServer == null || item.getValue()[1] > maxWeightServer.getValue()[1]) {
        maxWeightServer = item;
      }
      currentBrokers.put(
          item.getKey(), new int[] {item.getValue()[0], item.getValue()[1] + item.getValue()[0]});
    }
    assert maxWeightServer != null;
    currentBrokers.put(
        maxWeightServer.getKey(),
        new int[] {maxWeightServer.getValue()[0], maxWeightServer.getValue()[1] - allWeight});
    setCurrentBrokers(currentBrokers);

    ArrayList<Integer> partitionList = new ArrayList<>();
    for (PartitionInfo partitionInfo :
        cluster.partitionsForNode(Integer.parseInt(maxWeightServer.getKey()))) {
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
    Objects.requireNonNull(
        (String) configs.get("jmx_servers"), "You must configure jmx_servers correctly");
    nodeLoadClient = FACTORY.getOrCreate(NodeLoadClient.class, configs);
  }
  /** Change the weight of the node according to the current Poisson. */
  private synchronized void updateBrokersWeight() {
    Map<String, Double> allPoisson = nodeLoadClient.getLoadPoisson().getAllPoisson();

    for (Map.Entry<String, Double> entry : allPoisson.entrySet()) {
      if (!allBrokersWeight.containsKey(entry.getKey())) {
        allBrokersWeight.put(entry.getKey(), new int[] {(int) ((1 - entry.getValue()) * 20), 0});
      } else {
        allBrokersWeight.put(
            entry.getKey(),
            new int[] {
              (int) ((1 - entry.getValue()) * 20), allBrokersWeight.get(entry.getKey())[1]
            });
      }
    }
  }

  private synchronized int getAllWeight() {
    return allBrokersWeight.values().stream().mapToInt(vs -> vs[0]).sum();
  }

  private synchronized HashMap<String, int[]> getAllBrokersWeight() {
    return allBrokersWeight;
  }

  private synchronized void setCurrentBrokers(HashMap<String, int[]> currentBrokers) {
    allBrokersWeight = currentBrokers;
  }

  public NodeLoadClient getNodeLoadClient() {
    return nodeLoadClient;
  }

  public static NodeLoadClientFactory getFactory() {
    return FACTORY;
  }
}
