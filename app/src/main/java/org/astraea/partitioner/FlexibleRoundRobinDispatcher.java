package org.astraea.partitioner;

import static org.astraea.Utils.overSecond;
import static org.astraea.partitioner.nodeLoadMetric.PartitionerUtils.allPoisson;
import static org.astraea.partitioner.nodeLoadMetric.PartitionerUtils.toPositive;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.astraea.partitioner.nodeLoadMetric.NodeLoadClient;

public class FlexibleRoundRobinDispatcher implements Dispatcher {
  public static final String JMX_SERVERS_KEY = "jmx_servers";
  private static final double SLOP = 0.2;
  private NodeLoadClient nodeLoadClient;
  private final ConcurrentMap<String, AtomicInteger> topicCounterMap = new ConcurrentHashMap<>();
  private long lastTime;
  private Map<Integer, Double> allPoisson;
  private double avgPoisson;

  @Override
  public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
    var loadCount =
        Objects.requireNonNull(
            nodeLoadClient.loadSituation(clusterInfo), "OverLoadCount should not be null.");
    updatePoissonIfNeed(loadCount);

    var partitions = clusterInfo.partitions(topic);
    var numPartitions = partitions.size();
    var availablePartitions = clusterInfo.availablePartitions(topic);
    if (!availablePartitions.isEmpty()) {
      return choosePartition(availablePartitions, topic);
    } else {
      var nextValue = nextValue(topic);
      // no partitions are available, give a non-available partition
      return toPositive(nextValue) % numPartitions;
    }
  }

  @Override
  public void configure(Configuration config) {
    nodeLoadClient = new NodeLoadClient(config.map(JMX_SERVERS_KEY, ",", ":", Integer::valueOf));
  }

  @Override
  public void close() {}

  private int nextValue(String topic) {
    var counter = topicCounterMap.computeIfAbsent(topic, k -> new AtomicInteger(0));
    return counter.getAndIncrement();
  }

  private void updatePoissonIfNeed(Map<Integer, Integer> loadCount) {
    if (overSecond(lastTime, 1)) {
      lastTime = System.currentTimeMillis();
      allPoisson = allPoisson(loadCount);
      avgPoisson = allPoisson.values().stream().reduce(0.0, Double::sum) / allPoisson.size();
    }
  }

  private int choosePartition(List<PartitionInfo> availablePartitions, String topic) {
    var nextValue = nextValue(topic);
    var part = toPositive(nextValue) % availablePartitions.size();
    var partitionInfo = availablePartitions.get(part);
    if (chooseOrNot(allPoisson.get(partitionInfo.leader().id()))) {
      return partitionInfo.partition();
    } else {
      // no partitions are available, give a non-available partition
      return toPositive(nextValue) % availablePartitions.size();
    }
  }

  private boolean chooseOrNot(double brokerPoisson) {
    return !(brokerPoisson >= (1 + SLOP) * avgPoisson);
  }
}
