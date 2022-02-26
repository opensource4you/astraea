package org.astraea.partitioner.smoothPartitioner;

import static org.astraea.Utils.overSecond;
import static org.astraea.partitioner.nodeLoadMetric.PartitionerUtils.allPoisson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.astraea.partitioner.ClusterInfo;
import org.astraea.partitioner.nodeLoadMetric.NodeLoadClient;

/**
 * Based on the jmx metrics obtained from Kafka, it records the load status of the node over a
 * period of time. Predict the future status of each node through the poisson of the load status.
 * Finally, the result of poisson is used as the weight to perform smooth weighted RoundRobin.
 */
public class SmoothWeightPartitioner implements Partitioner {

  /**
   * Record the current weight of each node according to Poisson calculation and the weight after
   * partitioner calculation.
   */
  private final ConcurrentMap<Integer, SmoothWeightServer> brokersWeight =
      new ConcurrentHashMap<>();

  private NodeLoadClient nodeLoadClient;
  private long lastTime = -1;
  private final Random rand = new Random();
  private final Lock lock = new ReentrantLock();
  private int weightSum = -1;

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    Map<Integer, Integer> loadCount;
    loadCount = nodeLoadClient.loadSituation(ClusterInfo.of(cluster));
    Objects.requireNonNull(loadCount, "OverLoadCount should not be null.");
    updateWeightIfNeed(loadCount);

    SmoothWeightServer maxWeightServer = null;

    Iterator<Integer> iterator = brokersWeight.keySet().iterator();
    var currentWrightSum = weightSum;
    while (iterator.hasNext()) {
      SmoothWeightServer smoothWeightServer = brokersWeight.get(iterator.next());
      if (smoothWeightServer != null) {
        weightSum += smoothWeightServer.currentWeight;
        if (maxWeightServer == null) {
          maxWeightServer = smoothWeightServer;
        }
        if (smoothWeightServer.originalWeight() > maxWeightServer.originalWeight()) {
          maxWeightServer = smoothWeightServer;
        }
      }
    }

    Objects.requireNonNull(maxWeightServer, "MaxWeightServer should not be null.");
    maxWeightServer.updateOriginalWeight(currentWrightSum);

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
        .partitionsForNode(maxWeightServer.brokerID)
        .forEach(partitionInfo -> partitionList.add(partitionInfo.partition()));
    return partitionList.get(rand.nextInt(partitionList.size()));
  }

  @Override
  public void close() {
    nodeLoadClient.close();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    var jmxAddresses =
        Objects.requireNonNull(
            configs.get("jmx_servers").toString(), "You must configure jmx_servers correctly");
    var list = Arrays.asList((jmxAddresses).split(","));
    HashMap<String, Integer> mapAddress = new HashMap<>();
    list.forEach(
        str ->
            mapAddress.put(
                Arrays.asList(str.split(":")).get(0),
                Integer.parseInt(Arrays.asList(str.split(":")).get(1))));
    Objects.requireNonNull(mapAddress, "You must configure jmx_servers correctly.");

    nodeLoadClient = new NodeLoadClient(mapAddress);
  }

  /** Change the weight of the node according to the current Poisson. */
  void brokersWeight(Map<Integer, Double> poissonMap) {
    AtomicInteger sum = new AtomicInteger(0);
    poissonMap.forEach(
        (key, value) -> {
          var thoughPutAbility = nodeLoadClient.thoughPutComparison(key);
          if (!brokersWeight.containsKey(key))
            brokersWeight.putIfAbsent(
                key,
                new SmoothWeightServer(key, (int) Math.floor((1 - value) * 20 * thoughPutAbility)));
          else {
            var broker = brokersWeight.get(key);
            broker.currentWeight((int) Math.floor((1 - value) * 20 * thoughPutAbility));
            broker.originalWeight.updateAndGet((v) -> v + broker.currentWeight);
          }
          sum.addAndGet(brokersWeight.get(key).currentWeight);
        });
    weightSum = sum.get();
  }

  void updateWeightIfNeed(Map<Integer, Integer> loadCount) {
    if (overSecond(lastTime, 1) && lock.tryLock()) {
      try {
        lock.lock();
        brokersWeight(allPoisson(loadCount));
      } finally {
        lock.unlock();
        lastTime = System.currentTimeMillis();
      }
    }
  }

  Map<Integer, SmoothWeightServer> brokersWeight() {
    return brokersWeight;
  }

  private void subBrokerWeight(int brokerID, int subNumber) {
    brokersWeight.get(brokerID).updateOriginalWeight(subNumber);
  }

  private boolean memoryWarning(int brokerID) {
    return nodeLoadClient.memoryUsage(brokerID) >= 0.8;
  }

  static class SmoothWeightServer {
    private final int brokerID;
    private AtomicInteger originalWeight;
    private int currentWeight;

    SmoothWeightServer(int brokerID, int currentWeight) {
      this.brokerID = brokerID;
      this.currentWeight = currentWeight;
      this.originalWeight = new AtomicInteger(currentWeight);
    }

    int updateOriginalWeight(int subWeight) {
      return originalWeight.getAndUpdate((v) -> v - subWeight);
    }

    int originalWeight() {
      return originalWeight.get();
    }

    void currentWeight(int currentWeight) {
      this.currentWeight = currentWeight;
    }
  }
}
