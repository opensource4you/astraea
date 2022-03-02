package org.astraea.partitioner.smoothPartitioner;

import static org.astraea.Utils.overSecond;
import static org.astraea.partitioner.nodeLoadMetric.PartitionerUtils.allPoisson;
import static org.astraea.partitioner.nodeLoadMetric.PartitionerUtils.weightPoisson;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.astraea.partitioner.ClusterInfo;
import org.astraea.partitioner.Configuration;
import org.astraea.partitioner.NodeId;
import org.astraea.partitioner.nodeLoadMetric.NodeLoadClient;

/**
 * Based on the jmx metrics obtained from Kafka, it records the load status of the node over a
 * period of time. Predict the future status of each node through the poisson of the load status.
 * Finally, the result of poisson is used as the weight to perform smooth weighted RoundRobin.
 */
public class SmoothWeightPartitioner implements Partitioner {
  private static final String JMX_PORT = "jmx.defaultPort";
  private static final String JMX_SERVERS = "jmx.servers";

  //  Record the current weight of each node according to Poisson calculation and the weight after
  // partitioner calculation.
  private final ConcurrentMap<Integer, SmoothWeightServer> brokersWeight =
      new ConcurrentHashMap<>();

  private Optional<Integer> jmxPortDefault = Optional.empty();
  private final Map<NodeId, Integer> jmxPorts = new TreeMap<>();
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
    updateWeightIfNeed(loadCount);

    SmoothWeightServer maxWeightServer = null;

    var iterator = brokersWeight.keySet().iterator();
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
    var config =
        Configuration.of(
            configs.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));
    jmxPortDefault = config.integer(JMX_PORT);

    // seeks for custom jmx ports.
    config.entrySet().stream()
        .filter(e -> e.getKey().startsWith("broker."))
        .forEach(
            e ->
                jmxPorts.put(
                    NodeId.of(Integer.parseInt(e.getKey())), Integer.parseInt(e.getValue())));

    var jmxAddresses = config.string(JMX_SERVERS);
    var mapAddress = new HashMap<Integer, Integer>();

    if (jmxAddresses.isPresent()) {
      var list = Arrays.asList((jmxAddresses.get()).split(","));
      list.forEach(
          str -> {
            var arr = Arrays.asList(str.split("\\."));
            mapAddress.put(Integer.parseInt(arr.get(0)), Integer.parseInt(arr.get(1)));
          });
    }
    nodeLoadClient = new NodeLoadClient(mapAddress, jmxPortDefault);
  }

  /** Change the weight of the node according to the current Poisson. */
  synchronized void brokersWeight(Map<Integer, Double> poissonMap) {
    AtomicInteger sum = new AtomicInteger(0);
    poissonMap.forEach(
        (key, value) -> {
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

    int currentWeight() {
      return currentWeight;
    }
  }
}
