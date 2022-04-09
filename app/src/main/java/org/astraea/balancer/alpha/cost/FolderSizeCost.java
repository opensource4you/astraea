package org.astraea.balancer.alpha.cost;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.NodeInfo;
import org.astraea.cost.PartitionInfo;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.topic.Replica;
import org.astraea.topic.TopicAdmin;

public class FolderSizeCost implements CostFunction {
  static TopicAdmin admin;

  @Override
  public Map<TopicPartitionReplica, Double> cost(ClusterInfo clusterInfo) {
    Map<String, Long> totalSpace = new HashMap<>();
    totalSpace.put("/tmp/log-folder-0", 200 * 1073741824L); // 200GB
    totalSpace.put("/tmp/log-folder-1", 500 * 1073741824L); // 500GB
    totalSpace.put("/tmp/log-folder-2", 30 * 1073741824L); // 30GB

    var freeDiskSpace =
        new TreeMap<TopicPartitionReplica, Double>(
            Comparator.comparing(TopicPartitionReplica::brokerId)
                .thenComparing(TopicPartitionReplica::topic)
                .thenComparing(TopicPartitionReplica::partition));

    totalSpace.forEach(
        (path, totalSize) ->
            admin
                .replicas(admin.topicNames())
                .forEach(
                    (tp, replicas) ->
                        replicas.forEach(
                            r -> {
                              if (r.path().equals(path))
                                freeDiskSpace.put(
                                    new TopicPartitionReplica(
                                        tp.topic(), tp.partition(), r.broker()),
                                    Math.round(((double) r.size() / totalSpace.get(path)) * 1000.0)
                                        / 1000.0);
                            })));

    return freeDiskSpace;
  }

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  @Override
  public Fetcher fetcher() {
    return null;
  }

  public static void main(String[] args) throws InterruptedException {
    final String host = "192.168.103.39";
    final int port = 13764;
    admin = TopicAdmin.of(host + ":" + port);
    Cluster cluster = Cluster.bootstrap(List.of(InetSocketAddress.createUnresolved(host, port)));
    var all = new HashMap<Integer, List<HasBeanObject>>();
    ClusterInfo clusterInfo =
        new ClusterInfo() {
          @Override
          public List<NodeInfo> nodes() {
            List<NodeInfo> info = new ArrayList<>();
            admin
                .brokerIds()
                .forEach(
                    b -> {
                      admin.replicas(admin.topicNames());
                      info.add(NodeInfo.of(b, host, port));
                    });
            return info;
          }

          @Override
          public List<PartitionInfo> availablePartitions(String topic) {
            List<PartitionInfo> partitionInfo = new ArrayList<>();
            admin
                .replicas(Set.of(topic))
                .forEach(
                    (topicPartition, replicas) -> {
                      partitionInfo.add(
                          PartitionInfo.of(
                              topic,
                              topicPartition.partition(),
                              NodeInfo.of(
                                  replicas.stream()
                                      .filter(Replica::leader)
                                      .findFirst()
                                      .get()
                                      .broker(),
                                  host,
                                  port)));
                    });
            return partitionInfo;
          }

            @Override
            public Set<String> topics() {
                // TODO: fix this
                return null;
            }

            @Override
          public List<PartitionInfo> partitions(String topic) {
            List<PartitionInfo> partitionInfo = new ArrayList<>();
            admin
                .replicas(Set.of(topic))
                .forEach(
                    (topicPartition, replicas) -> {
                      partitionInfo.add(
                          PartitionInfo.of(
                              topic,
                              topicPartition.partition(),
                              NodeInfo.of(
                                  replicas.stream()
                                      .filter(Replica::leader)
                                      .findFirst()
                                      .get()
                                      .broker(),
                                  host,
                                  port)));
                    });
            return partitionInfo;
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
    clusterInfo
        .allBeans()
        .forEach((key, value) -> all.computeIfAbsent(key, k -> new ArrayList<>()).addAll(value));
    clusterInfo
        .allBeans()
        .forEach((key, value) -> all.computeIfAbsent(key, k -> new ArrayList<>()).addAll(value));

    CostFunction costFunction = new FolderSizeCost();
    costFunction
        .cost(clusterInfo)
        .forEach(
            (key, value) ->
                System.out.println(
                    key.brokerId() + "-" + key.topic() + "-" + key.partition() + ": " + value));
  }
}
