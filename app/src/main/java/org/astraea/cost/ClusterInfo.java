package org.astraea.cost;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.Utils;
import org.astraea.metrics.HasBeanObject;

public interface ClusterInfo {

  static ClusterInfo of(org.apache.kafka.common.Cluster cluster) {
    return new ClusterInfo() {
      long lastFetchTime = 0L;
      List<PartitionInfo> availablePartitions;

      @Override
      public List<NodeInfo> nodes() {
        return cluster.nodes().stream().map(NodeInfo::of).collect(Collectors.toUnmodifiableList());
      }

      public Set<String> topics() {
        return cluster.topics();
      }

      @Override
      public List<PartitionInfo> availablePartitions(String topic) {
        if (Utils.overSecond(lastFetchTime, 1)) {
          try {
            availablePartitions =
                cluster.availablePartitionsForTopic(topic).stream()
                    .map(PartitionInfo::of)
                    .collect(Collectors.toUnmodifiableList());
          } finally {
            lastFetchTime = System.currentTimeMillis();
          }
        }
        return availablePartitions;
      }

      @Override
      public List<PartitionInfo> partitions(String topic) {
        return cluster.partitionsForTopic(topic).stream()
            .map(PartitionInfo::of)
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public Collection<HasBeanObject> beans(int brokerId) {
        return List.of();
      }

      @Override
      public Map<Integer, Collection<HasBeanObject>> allBeans() {
        return Map.of();
      }
    };
  }

  /**
   * merge the beans into cluster information
   *
   * @param cluster cluster information
   * @param beans extra beans
   * @return a new cluster information with extra beans
   */
  static ClusterInfo of(ClusterInfo cluster, Map<Integer, Collection<HasBeanObject>> beans) {
    var all = new HashMap<Integer, List<HasBeanObject>>();
    cluster
        .allBeans()
        .forEach((key, value) -> all.computeIfAbsent(key, k -> new ArrayList<>()).addAll(value));
    beans.forEach((key, value) -> all.computeIfAbsent(key, k -> new ArrayList<>()).addAll(value));
    return new ClusterInfo() {
      long lastFetchTime = 0L;
      List<PartitionInfo> availablePartitions;

      @Override
      public List<NodeInfo> nodes() {
        return cluster.nodes();
      }

      @Override
      public List<PartitionInfo> availablePartitions(String topic) {
        if (Utils.overSecond(lastFetchTime, 1)) {
          try {
            availablePartitions = cluster.availablePartitions(topic);
          } finally {
            lastFetchTime = System.currentTimeMillis();
          }
        }
        return availablePartitions;
      }

      @Override
      public Set<String> topics() {
        return cluster.topics();
      }

      @Override
      public List<PartitionInfo> partitions(String topic) {
        return cluster.partitions(topic);
      }

      @Override
      public Collection<HasBeanObject> beans(int brokerId) {
        return all.getOrDefault(brokerId, List.of());
      }

      @Override
      public Map<Integer, Collection<HasBeanObject>> allBeans() {
        return Collections.unmodifiableMap(all);
      }
    };
  }

  /**
   * find the node associated to specify node and port. Normally, the node + port should be unique
   * in cluster.
   *
   * @param host hostname
   * @param port client port
   * @return the node information. It throws NoSuchElementException if specify node and port is not
   *     associated to any node
   */
  default NodeInfo node(String host, int port) {
    return nodes().stream()
        .filter(n -> n.host().equals(host) && n.port() == port)
        .findAny()
        .orElseThrow(() -> new NoSuchElementException(host + ":" + port + " is nonexistent"));
  }

  /**
   * find the node associated to node id.
   *
   * @param id node id
   * @return the node information. It throws NoSuchElementException if specify node id is not
   *     associated to any node
   */
  default NodeInfo node(int id) {
    return nodes().stream()
        .filter(n -> n.id() == id)
        .findAny()
        .orElseThrow(() -> new IllegalArgumentException(id + " is nonexistent"));
  }

  /** @return The known set of nodes */
  List<NodeInfo> nodes();

  /**
   * Get the list of available partitions for this topic
   *
   * @param topic The topic name
   * @return A list of partitions
   */
  List<PartitionInfo> availablePartitions(String topic);

  /**
   * All topic names
   *
   * @return return a set of topic names
   */
  Set<String> topics();

  /**
   * Get the list of partitions for this topic
   *
   * @param topic The topic name
   * @return A list of partitions
   */
  List<PartitionInfo> partitions(String topic);

  /**
   * @param brokerId broker id
   * @return return the metrics of broker. It returns empty collection if the broker id is
   *     nonexistent
   */
  Collection<HasBeanObject> beans(int brokerId);

  /** @return all beans of all brokers */
  Map<Integer, Collection<HasBeanObject>> allBeans();
}
