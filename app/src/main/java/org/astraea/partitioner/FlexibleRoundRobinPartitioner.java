package org.astraea.partitioner;

import static org.astraea.Utils.overSecond;
import static org.astraea.partitioner.nodeLoadMetric.PartitionerUtils.allPoisson;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.astraea.partitioner.nodeLoadMetric.NodeLoadClient;

/** We'll skip heavily loaded partitions, simple and effective. */
public class FlexibleRoundRobinPartitioner implements AstraeaPartitioner {
  private final ConcurrentMap<String, AtomicInteger> topicCounterMap = new ConcurrentHashMap<>();
  private long lastTime;
  private final NodeLoadClient nodeLoadClient;
  private Map<Integer, Double> allPoisson;
  private double avgPoisson;
  private final double slop = 0.2;

  FlexibleRoundRobinPartitioner(NodeLoadClient nodeLoadClient) {
    this.nodeLoadClient = nodeLoadClient;
    lastTime = -1;
  }

  @Override
  public int loadPartition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    var loadCount = nodeLoadClient.loadSituation(cluster);
    Objects.requireNonNull(loadCount, "OverLoadCount should not be null.");
    updatePoissonIfNeed(loadCount);

    var partitions = cluster.partitionsForTopic(topic);
    var numPartitions = partitions.size();
    var availablePartitions = cluster.availablePartitionsForTopic(topic);
    if (!availablePartitions.isEmpty()) {
      return choosePartition(availablePartitions, topic);
    } else {
      var nextValue = nextValue(topic);
      // no partitions are available, give a non-available partition
      return Utils.toPositive(nextValue) % numPartitions;
    }
  }

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
    var part = Utils.toPositive(nextValue) % availablePartitions.size();
    var partitionInfo = availablePartitions.get(part);
    if (chooseOrNot(allPoisson.get(partitionInfo.leader().id()))) {
      return partitionInfo.partition();
    } else return choosePartition(availablePartitions, topic);
  }

  private boolean chooseOrNot(double brokerPoisson) {
    return !(brokerPoisson >= (1 + slop) * avgPoisson);
  }
}
