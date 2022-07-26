/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.cost;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.KafkaMetrics;
import org.astraea.app.metrics.broker.HasCount;
import org.astraea.app.metrics.broker.HasValue;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.partitioner.Configuration;

public class MoveCost implements HasMoveCost {
  public MoveCost(Configuration configuration) {
    this.duration =
        Duration.ofSeconds(Integer.parseInt(configuration.string("metrics.duration").orElse("30")));
    this.minDiskFreeSpace =
        Double.valueOf(configuration.string("min.free.space.percentage").orElse("0.2"));
    this.totalBrokerCapacity = ReplicaSizeCost.convert(configuration.requireString(BROKERCAPACITY));
    this.brokerBandwidthCap =
        ReplicaDiskInCost.convert(configuration.requireString(BROKERBANDWIDTH));
  }

  static class ReplicaMigrateInfo {
    TopicPartition topicPartition;
    int brokerSource;
    int brokerSink;
    String pathSource;
    String pathSink;

    public ReplicaMigrateInfo(
        TopicPartition topicPartition,
        int brokerSource,
        int brokerSink,
        String pathSource,
        String pathSink) {
      this.topicPartition = topicPartition;
      this.brokerSource = brokerSource;
      this.brokerSink = brokerSink;
      this.pathSource = pathSource;
      this.pathSink = pathSink;
    }

    TopicPartitionReplica sourceTPR() {
      return TopicPartitionReplica.of(
          topicPartition.topic(), topicPartition.partition(), brokerSource);
    }
  }

  static final String BROKERBANDWIDTH = "brokerBandwidthConfig";
  static final String BROKERCAPACITY = "brokerCapacityConfig";
  static final String UNKNOWN = "unknown";
  final Double minDiskFreeSpace;
  final Duration duration;
  Map<Integer, Map<String, Integer>> totalBrokerCapacity;
  Map<Integer, Integer> brokerBandwidthCap;

  private boolean filterBean(HasBeanObject hasBeanObject, String metricName) {
    var beanObject = hasBeanObject.beanObject();
    return beanObject != null
        && beanObject.properties().containsKey("name")
        && beanObject.properties().get("name").equals(metricName);
  }

  public Map<Integer, Double> brokerTrafficMetrics(
      ClusterBean clusterBean, String metricName, Duration sampleWindow) {
    return clusterBean.all().entrySet().stream()
        .map(
            brokerMetrics -> {
              var sizeTimeSeries =
                  brokerMetrics.getValue().stream()
                      .filter(hasBeanObject -> filterBean(hasBeanObject, metricName))
                      .map(hasBeanObject -> (HasCount) hasBeanObject)
                      .sorted(Comparator.comparingLong(HasBeanObject::createdTimestamp).reversed())
                      .collect(Collectors.toUnmodifiableList());
              var latestSize =
                  sizeTimeSeries.stream().findFirst().orElseThrow(NoSuchElementException::new);
              var windowSize =
                  sizeTimeSeries.stream()
                      .dropWhile(
                          bean ->
                              bean.createdTimestamp()
                                  > latestSize.createdTimestamp() - sampleWindow.toMillis())
                      .findFirst()
                      .orElseThrow();
              var dataRate =
                  ((double) (latestSize.count() - windowSize.count()))
                      / ((double) (latestSize.createdTimestamp() - windowSize.createdTimestamp())
                          / 1000)
                      / 1024.0
                      / 1024.0;
              return Map.entry(brokerMetrics.getKey(), dataRate);
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  boolean checkBrokerInTraffic(
      Map<TopicPartitionReplica, Double> replicaDataRate,
      List<ReplicaMigrateInfo> migratedReplicas,
      ClusterBean clusterBean) {
    // TODO: need replicaOutRate and estimated migrate traffic.
    var brokerBandwidthCap = this.brokerBandwidthCap;
    AtomicReference<Boolean> overflow = new AtomicReference<>(false);
    var brokerBytesIn = brokerTrafficMetrics(clusterBean, "BytesInPerSec", duration);
    var replicationBytesInPerSec =
        brokerTrafficMetrics(clusterBean, "ReplicationBytesInPerSec", duration);
    var replicationBytesOutPerSec =
        brokerTrafficMetrics(clusterBean, "ReplicationBytesOutPerSec", duration);
    var brokerTrafficChange = new HashMap<Integer, Double>();
    migratedReplicas.forEach(
        replicaMigrateInfo -> {
          // TODO: need replicaOutRate
          var sourceChange = brokerTrafficChange.getOrDefault(replicaMigrateInfo.brokerSource, 0.0);
          //    - replicaDataRate.getOrDefault(replicaMigrateInfo.sourceTPR(), 0.0);
          var sinkChange =
              brokerTrafficChange.getOrDefault(replicaMigrateInfo.brokerSink, 0.0)
                  + replicaDataRate.getOrDefault(replicaMigrateInfo.sourceTPR(), 0.0);
          brokerTrafficChange.put(replicaMigrateInfo.brokerSource, sourceChange);
          brokerTrafficChange.put(replicaMigrateInfo.brokerSink, sinkChange);
        });
    var brokerReplicaIn =
        replicaDataRate.entrySet().stream()
            .collect(Collectors.groupingBy(e -> e.getKey().brokerId()))
            .entrySet()
            .stream()
            .map(
                x ->
                    Map.entry(
                        x.getKey(), x.getValue().stream().mapToDouble(Map.Entry::getValue).sum()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    brokerBandwidthCap.forEach(
        (broker, bandwidth) -> {
          if (brokerBytesIn.get(broker)
                  + replicationBytesInPerSec.get(broker)
                  + brokerReplicaIn.get(broker)
                  + brokerTrafficChange.getOrDefault(broker, 0.0)
              > bandwidth) overflow.set(true);
        });
    return overflow.get();
  }

  boolean checkFolderSize(
      Map<TopicPartitionReplica, Long> replicaSize, List<ReplicaMigrateInfo> distributionChange) {
    AtomicReference<Boolean> overflow = new AtomicReference<>(false);
    var pathSizeChange = new HashMap<Map.Entry<Integer, String>, Long>();
    distributionChange.forEach(
        replicaMigrateInfo -> {
          var sourceSizeChange =
              pathSizeChange.getOrDefault(
                  Map.entry(replicaMigrateInfo.brokerSource, replicaMigrateInfo.pathSource), 0L);
          // - replicaSize.get(replicaMigrateInfo.sourceTPR());
          var sinkSizeChange =
              pathSizeChange.getOrDefault(
                      Map.entry(replicaMigrateInfo.brokerSink, replicaMigrateInfo.pathSink), 0L)
                  + replicaSize.get(replicaMigrateInfo.sourceTPR());
          pathSizeChange.put(
              Map.entry(replicaMigrateInfo.brokerSource, replicaMigrateInfo.pathSource),
              sourceSizeChange);
          pathSizeChange.put(
              Map.entry(replicaMigrateInfo.brokerSink, replicaMigrateInfo.pathSink),
              sinkSizeChange);
        });
    totalBrokerCapacity.forEach(
        (broker, pathSize) ->
            pathSize.forEach(
                (path, size) -> {
                  if ((totalBrokerCapacity.get(broker).getOrDefault(path, 0)
                              + pathSizeChange.getOrDefault(Map.entry(broker, path), 0L))
                          / 1024.0
                          / 1024.0
                          / size
                      > (1 - minDiskFreeSpace)) overflow.set(true);
                }));
    return overflow.get();
  }

  boolean checkMigrateSpeed() {
    // TODO: Estimate the traffic available to a replica
    AtomicReference<Boolean> overflow = new AtomicReference<>(false);
    return overflow.get();
  }

  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.of(
        client ->
            Stream.of(
                    client12 ->
                        List.of(
                            KafkaMetrics.BrokerTopic.BytesInPerSec.fetch(client12),
                            KafkaMetrics.BrokerTopic.BytesOutPerSec.fetch(client12),
                            KafkaMetrics.BrokerTopic.ReplicationBytesInPerSec.fetch(client12),
                            KafkaMetrics.BrokerTopic.ReplicationBytesOutPerSec.fetch(client12)),
                    (Fetcher) KafkaMetrics.TopicPartition.Size::fetch)
                .flatMap(f -> f.fetch(client).stream())
                .collect(Collectors.toUnmodifiableList()));
  }

  @Override
  public ClusterCost clusterCost(
      ClusterInfo originClusterInfo, ClusterInfo newClusterInfo, ClusterBean clusterBean) {
    if (overflow(originClusterInfo, newClusterInfo, clusterBean)) return () -> 100.0;
    var migratedReplicas = getMigrateReplicas(originClusterInfo, newClusterInfo, true);
    var replicaSize = getReplicaSize(clusterBean);
    var replicaDataRate = replicaDataRate(clusterBean, duration);
    var brokerBytesInPerSec = brokerTrafficMetrics(clusterBean, "BytesInPerSec", duration);
    var brokerBytesOutPerSec = brokerTrafficMetrics(clusterBean, "BytesOutPerSec", duration);
    var replicationBytesInPerSec =
        brokerTrafficMetrics(clusterBean, "ReplicationBytesInPerSec", duration);
    var replicationBytesOutPerSec =
        brokerTrafficMetrics(clusterBean, "ReplicationBytesOutPerSec", duration);
    var availableMigrateBandwidth =
        brokerBandwidthCap.entrySet().stream()
            .map(
                brokerBandWidth ->
                    Map.entry(
                        brokerBandWidth.getKey(),
                        brokerBandWidth.getValue()
                            - brokerBytesInPerSec.get(brokerBandWidth.getKey())
                            - replicationBytesInPerSec.get(brokerBandWidth.getKey())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var brokerMigrateInSize =
        migratedReplicas.stream()
            .map(
                replicaMigrateInfo ->
                    Map.entry(
                        replicaMigrateInfo.brokerSink,
                        (double) replicaSize.get(replicaMigrateInfo.sourceTPR())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Double::sum));
    var trafficSeries =
        migratedReplicas.stream()
            .map(x -> replicaDataRate.get(x.sourceTPR()))
            .sorted()
            .filter(x -> x != 0.0)
            .collect(Collectors.toList());
    var meanTrafficSeries = trafficSeries.stream().mapToDouble(x -> x).sum() / trafficSeries.size();
    var SDTrafficSeries =
        Math.sqrt(
            trafficSeries.stream()
                    .mapToDouble(score -> Math.pow((score - meanTrafficSeries), 2))
                    .sum()
                / trafficSeries.size());

    var totalMigrateTraffic = trafficSeries.stream().mapToDouble(x -> x).sum();
    var totalReplicaTrafficInSink =
        replicaDataRate.entrySet().stream()
            .filter(x -> brokerMigrateInSize.containsKey(x.getKey().brokerId()))
            .mapToDouble(Map.Entry::getValue)
            .sum();
    var meanMigrateSize = trafficSeries.stream().mapToDouble(x -> x).sum() / trafficSeries.size();
    var sdMigrateSize =
        Math.sqrt(
            trafficSeries.stream()
                    .mapToDouble(score -> Math.pow((score - meanMigrateSize), 2))
                    .sum()
                / trafficSeries.size());
    var brokerMigrateScore = CostUtils.TScore(brokerMigrateInSize);
    var totalMigrateSize =
        brokerMigrateInSize.values().stream().mapToDouble(x -> x / 1024.0 / 1024.0).sum();
    var total =
        totalBrokerCapacity.values().stream()
                .mapToDouble(x -> x.values().stream().mapToDouble(y -> y).sum())
                .sum()
            / 1024.0
            / 1024.0;
    var totalMigrateSizeScore = totalMigrateSize / total > 1 ? 1 : totalMigrateSize / total;
    if (replicaDataRate.containsValue(-1.0)) {
      return () -> 100.0;
    }
    return () -> SDTrafficSeries;
  }

  public Map<TopicPartitionReplica, Long> getReplicaSize(ClusterBean clusterBean) {
    return clusterBean.mapByReplica().entrySet().stream()
        .flatMap(
            e ->
                e.getValue().stream()
                    .filter(x -> x instanceof HasValue)
                    .filter(x -> x.beanObject().domainName().equals("kafka.log"))
                    .filter(x -> x.beanObject().properties().get("type").equals("Log"))
                    .filter(x -> x.beanObject().properties().get("name").equals("Size"))
                    .map(x -> (HasValue) x)
                    .map(x -> Map.entry(e.getKey(), x.value())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x1, x2) -> x2));
  }

  public Map<TopicPartitionReplica, Double> replicaDataRate(
      ClusterBean clusterBean, Duration sampleWindow) {
    return clusterBean.mapByReplica().entrySet().parallelStream()
        .map(
            metrics -> {
              // calculate the increase rate over a specific window of time
              var sizeTimeSeries =
                  metrics.getValue().stream()
                      .filter(bean -> bean instanceof HasValue)
                      .filter(bean -> bean.beanObject().properties().get("type").equals("Log"))
                      .filter(bean -> bean.beanObject().properties().get("name").equals("Size"))
                      .map(bean -> (HasValue) bean)
                      .sorted(Comparator.comparingLong(HasBeanObject::createdTimestamp).reversed())
                      .collect(Collectors.toUnmodifiableList());
              var latestSize = sizeTimeSeries.stream().findFirst().orElseThrow();
              var windowSize =
                  sizeTimeSeries.stream()
                      .dropWhile(
                          bean ->
                              bean.createdTimestamp()
                                  > latestSize.createdTimestamp() - sampleWindow.toMillis())
                      .findFirst()
                      .orElse(latestSize);
              var dataRate =
                  ((double) (latestSize.value() - windowSize.value()))
                      / ((double) (latestSize.createdTimestamp() - windowSize.createdTimestamp())
                          / 1000)
                      / 1024.0
                      / 1024.0;
              if (dataRate < 0)
                // retention.size is triggered
                dataRate = -1.0;
              if (latestSize == windowSize) dataRate = 0.0;
              return Map.entry(metrics.getKey(), dataRate);
            })
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public boolean overflow(
      ClusterInfo originClusterInfo, ClusterInfo newClusterInfo, ClusterBean clusterBean) {
    var distributionChange = getMigrateReplicas(originClusterInfo, newClusterInfo, false);
    var migratedReplicas = getMigrateReplicas(originClusterInfo, newClusterInfo, true);
    var replicaSize = getReplicaSize(clusterBean);
    var replicaDataRate = replicaDataRate(clusterBean, duration);
    return checkBrokerInTraffic(replicaDataRate, migratedReplicas, clusterBean)
        || checkFolderSize(replicaSize, distributionChange)
        || checkMigrateSpeed();
  }

  public List<String> totalMigrateSize() {
    return null;
  }

  public List<String> EstimatedMigrateTime() {
    return null;
  }

  public List<ReplicaMigrateInfo> getMigrateReplicas(
      ClusterInfo originClusterInfo, ClusterInfo newClusterInfo, boolean fromLeader) {
    var leaderReplicas =
        originClusterInfo.topics().stream()
            .map(originClusterInfo::availableReplicaLeaders)
            .flatMap(
                replicaInfos ->
                    replicaInfos.stream()
                        .map(
                            replicaInfo ->
                                Map.entry(
                                    TopicPartition.of(replicaInfo.topic(), replicaInfo.partition()),
                                    Map.entry(
                                        TopicPartitionReplica.of(
                                            replicaInfo.topic(),
                                            replicaInfo.partition(),
                                            replicaInfo.nodeInfo().id()),
                                        replicaInfo.dataFolder().orElse(UNKNOWN)))))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var beforeMigrate =
        originClusterInfo.topics().stream()
            .map(originClusterInfo::availableReplicas)
            .flatMap(
                replicaInfos ->
                    replicaInfos.stream()
                        .map(
                            replicaInfo ->
                                Map.entry(
                                    TopicPartitionReplica.of(
                                        replicaInfo.topic(),
                                        replicaInfo.partition(),
                                        replicaInfo.nodeInfo().id()),
                                    replicaInfo.dataFolder().orElse(UNKNOWN))))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var afterMigrate =
        newClusterInfo.topics().stream()
            .map(newClusterInfo::availableReplicas)
            .flatMap(
                replicaInfos ->
                    replicaInfos.stream()
                        .map(
                            replicaInfo ->
                                Map.entry(
                                    TopicPartitionReplica.of(
                                        replicaInfo.topic(),
                                        replicaInfo.partition(),
                                        replicaInfo.nodeInfo().id()),
                                    replicaInfo.dataFolder().orElse(UNKNOWN))))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    if (fromLeader)
      return afterMigrate.entrySet().stream()
          .filter(newTPR -> !beforeMigrate.containsKey(newTPR.getKey()))
          .map(
              newTPR -> {
                var tp = TopicPartition.of(newTPR.getKey().topic(), newTPR.getKey().partition());
                var sinkBroker = newTPR.getKey().brokerId();
                return new ReplicaMigrateInfo(
                    tp,
                    leaderReplicas.get(tp).getKey().brokerId(),
                    sinkBroker,
                    leaderReplicas.get(tp).getValue(),
                    newTPR.getValue());
              })
          .collect(Collectors.toList());
    else {
      var sourceChange =
          beforeMigrate.entrySet().stream()
              .filter(oldTPR -> !afterMigrate.containsKey(oldTPR.getKey()))
              .map(
                  oldTPR ->
                      Map.entry(
                          TopicPartition.of(oldTPR.getKey().topic(), oldTPR.getKey().partition()),
                          Map.of(oldTPR.getKey().brokerId(), oldTPR.getValue())))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x1, x2) -> x1));
      var sinkChange =
          afterMigrate.entrySet().stream()
              .filter(newTPR -> !beforeMigrate.containsKey(newTPR.getKey()))
              .map(
                  newTPR ->
                      Map.entry(
                          TopicPartition.of(newTPR.getKey().topic(), newTPR.getKey().partition()),
                          Map.of(newTPR.getKey().brokerId(), newTPR.getValue())))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x1, x2) -> x1));
      var change = new ArrayList<ReplicaMigrateInfo>();
      if (sourceChange.keySet().containsAll(sinkChange.keySet())
          && sourceChange.values().size() == sinkChange.values().size())
        sourceChange.forEach(
            (tp, brokerPathMap) -> {
              var sinkBrokerPaths = new HashMap<>(sinkChange.get(tp));
              brokerPathMap.forEach(
                  (sourceBroker, sourcePath) -> {
                    var sinkBrokerPath = sinkBrokerPaths.entrySet().iterator().next();
                    change.add(
                        new ReplicaMigrateInfo(
                            tp,
                            sourceBroker,
                            sinkBrokerPath.getKey(),
                            sourcePath,
                            sinkBrokerPath.getValue()));
                    sinkBrokerPaths.remove(sinkBrokerPath.getKey());
                  });
            });
      return change;
    }
  }
}
