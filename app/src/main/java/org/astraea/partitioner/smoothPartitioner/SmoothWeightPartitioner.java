package org.astraea.partitioner.smoothPartitioner;

import static org.astraea.Utils.overSecond;
import static org.astraea.partitioner.nodeLoadMetric.PartitionerUtils.allPoisson;
import static org.astraea.partitioner.nodeLoadMetric.PartitionerUtils.weightPoisson;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
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
    var loadCount = nodeLoadClient.loadSituation(ClusterInfo.of(cluster));
    Objects.requireNonNull(loadCount, "OverLoadCount should not be null.");
    var availableNodeID =
        cluster.availablePartitionsForTopic(topic).stream()
            .map(PartitionInfo::leader)
            .map(Node::id)
            .distinct()
            .collect(Collectors.toList());
    updateWeightIfNeed(loadCount, availableNodeID);

    SmoothWeightServer maxWeightServer = null;

    Iterator<Integer> iterator = brokersWeight.keySet().iterator();
    var currentWrightSum = weightSum;
    while (iterator.hasNext()) {
      var smoothWeightServer = brokersWeight.get(iterator.next());
      if (smoothWeightServer != null) {
        smoothWeightServer.originalWeight.updateAndGet((v) -> v + smoothWeightServer.currentWeight);

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

    var partitions = cluster.partitionsForNode(maxWeightServer.brokerID);

    return partitions.get(rand.nextInt(partitions.size())).partition();
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
  synchronized void brokersWeight(Map<Integer, Double> poissonMap, List<Integer> availableNodeID) {
    AtomicInteger sum = new AtomicInteger(0);
    poissonMap.forEach(
        (key, value) -> {
          if (availableNodeID.stream().anyMatch(ID -> ID.equals(key))) {
            var thoughPutAbility = nodeLoadClient.thoughPutComparison(key);
            if (!brokersWeight.containsKey(key))
              brokersWeight.putIfAbsent(
                  key, new SmoothWeightServer(key, weightPoisson(value, thoughPutAbility)));
            else {
              var broker = brokersWeight.get(key);
              broker.currentWeight(weightPoisson(value, thoughPutAbility));
            }
            if (memoryWarning(key)) {
              subBrokerWeight(key, 100);
            }
            sum.addAndGet(brokersWeight.get(key).currentWeight);
          }
        });

    weightSum = sum.get();
  }

  void updateWeightIfNeed(Map<Integer, Integer> loadCount, List<Integer> availableNodeID) {
    if (overSecond(lastTime, 1) && lock.tryLock()) {
      try {
        lock.lock();
        brokersWeight(allPoisson(loadCount), availableNodeID);
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

    int currentWeight() {
      return currentWeight;
    }
  }
}
