package org.astraea.partitioner;

import static org.astraea.Utils.overSecond;
import static org.astraea.partitioner.nodeLoadMetric.PartitionerUtils.allPoisson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.Cluster;
import org.astraea.partitioner.nodeLoadMetric.NodeLoadClient;

/**
 * Based on the jmx metrics obtained from Kafka, it records the load status of the node over a
 * period of time. Predict the future status of each node through the poisson of the load status.
 * Finally, the result of poisson is used as the weight to perform smooth weighted RoundRobin.
 */
public class SmoothWeightPartitioner implements AstraeaPartitioner {
  /**
   * Record the current weight of each node according to Poisson calculation and the weight after
   * partitioner calculation.
   */
  private final Map<Integer, int[]> brokersWeight = new HashMap<>();

  private final NodeLoadClient nodeLoadClient;

  private long lastTime = -1;
  private final Random rand = new Random();

  SmoothWeightPartitioner(NodeLoadClient nodeLoadClient) {
    this.nodeLoadClient = nodeLoadClient;
  }

  @Override
  public int loadPartition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    Map<Integer, Integer> loadCount = nodeLoadClient.loadSituation(cluster);

    Objects.requireNonNull(loadCount, "OverLoadCount should not be null.");
    updateWeightIfNeed(loadCount);
    AtomicReference<Map.Entry<Integer, int[]>> maxWeightServer = new AtomicReference<>();
    brokersWeight.forEach(
        (nodeID, weight) -> {
          if (maxWeightServer.get() == null || weight[1] > maxWeightServer.get().getValue()[1]) {
            maxWeightServer.set(Map.entry(nodeID, weight));
          }
          brokersWeight.put(nodeID, new int[] {weight[0], weight[1] + weight[0]});
        });
    Objects.requireNonNull(maxWeightServer.get(), "MaxWeightServer should not be null.");
    brokersWeight.put(
        maxWeightServer.get().getKey(),
        new int[] {
          maxWeightServer.get().getValue()[0],
          maxWeightServer.get().getValue()[1] - allNodesWeight()
        });
    brokersWeight
        .keySet()
        .forEach(
            brokerID -> {
              if (memoryWarning(brokerID)) {
                subBrokerWeight(brokerID, 100);
              }
            });

    ArrayList<Integer> partitionList = new ArrayList<>();
    cluster
        .partitionsForNode(maxWeightServer.get().getKey())
        .forEach(partitionInfo -> partitionList.add(partitionInfo.partition()));
    return partitionList.get(rand.nextInt(partitionList.size()));
  }

  /** Change the weight of the node according to the current Poisson. */
  // visible for test
  public synchronized void brokersWeight(Map<Integer, Double> poissonMap) {
    poissonMap.forEach(
        (key, value) -> {
          var thoughPutAbility = nodeLoadClient.thoughPutComparison(key);
          if (!brokersWeight.containsKey(key)) {
            brokersWeight.put(key, new int[] {(int) ((1 - value) * 20 * thoughPutAbility), 0});
          } else {
            brokersWeight.put(
                key,
                new int[] {(int) ((1 - value) * 20 * thoughPutAbility), brokersWeight.get(key)[1]});
          }
        });
  }

  void updateWeightIfNeed(Map<Integer, Integer> loadCount) {
    if (overSecond(lastTime, 1)) {
      brokersWeight(allPoisson(loadCount));
      lastTime = System.currentTimeMillis();
    }
  }

  Map<Integer, int[]> brokersWeight() {
    return brokersWeight;
  }

  private void subBrokerWeight(int brokerID, int subNumber) {
    brokersWeight.put(
        brokerID,
        new int[] {brokersWeight.get(brokerID)[0], brokersWeight.get(brokerID)[1] - subNumber});
  }

  private synchronized int allNodesWeight() {
    return brokersWeight.values().stream().mapToInt(vs -> vs[0]).sum();
  }

  private boolean memoryWarning(int brokerID) {
    return nodeLoadClient.memoryUsage(brokerID) >= 0.8;
  }
}
