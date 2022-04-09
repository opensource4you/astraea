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
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.NodeInfo;
import org.astraea.cost.PartitionInfo;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.topic.Replica;
import org.astraea.topic.TopicAdmin;

public class ReplicaDiskInCost implements CostFunction {
  // private final Map<Integer, BrokerMetric> brokersMetric = new HashMap<>();
  static TopicAdmin admin;

  @Override
  public Map<TopicPartitionReplica, Double> cost(ClusterInfo clusterInfo)
      throws InterruptedException {
    // set broker bandwidth upper limit to 10 MB/s
    var bandwidthCap = 10;
    //  duration of the calculated
    var duration = 10;
    // a replica average data in rate (MB/s).
    var dataInRate =
        new TreeMap<TopicPartitionReplica, Double>(
            Comparator.comparing(TopicPartitionReplica::topic)
                .thenComparing(TopicPartitionReplica::partition));
    var dataInRateScore =
        new TreeMap<TopicPartitionReplica, Double>(
            Comparator.comparing(TopicPartitionReplica::topic)
                .thenComparing(TopicPartitionReplica::partition));

    Map<TopicPartitionReplica, Replica> oldReplicas = new HashMap<>();
    admin
        .replicas(admin.topicNames())
        .forEach(
            (tp, replica) -> {
              replica.forEach(
                  r ->
                      oldReplicas.put(
                          new TopicPartitionReplica(tp.topic(), tp.partition(), r.broker()), r));
            });
    TimeUnit.SECONDS.sleep(duration);
    Map<Integer, Integer> numberOfReplicaInBroker = new HashMap<>();
    admin.brokerIds().forEach(broker -> numberOfReplicaInBroker.put(broker, 0));
    admin
        .replicas(admin.topicNames())
        .forEach(
            (tp, replica) ->
                replica.forEach(
                    r -> {
                      var tpr = new TopicPartitionReplica(tp.topic(), tp.partition(), r.broker());
                      numberOfReplicaInBroker.put(
                          r.broker(), numberOfReplicaInBroker.get(r.broker()) + 1);
                      dataInRate.put(
                          tpr,
                          (double) (r.size() - oldReplicas.get(tpr).size()) / 1048576 / duration);
                    }));
    dataInRate.forEach(
        (tpr, rate) -> {
          var score = rate / bandwidthCap * numberOfReplicaInBroker.get(tpr.brokerId());
          if (score >= 1) score = 1;
          dataInRateScore.put(tpr, score);
        });
    return dataInRateScore;
  }

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  @Override
  public Fetcher fetcher() {
    return null;
  }

  public static void main(String[] args) throws InterruptedException {
    final String host = "192.168.103.39";
    final int port = 15148;
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
                      replicas.stream().filter(Replica::leader);
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
    CostFunction costFunction = new ReplicaDiskInCost();
    costFunction
        .cost(clusterInfo)
        .forEach(
            (tpr, score) -> {
              System.out.println(
                  tpr.topic() + "-" + tpr.partition() + "-" + tpr.brokerId() + " : " + score);
            });
  }
}
