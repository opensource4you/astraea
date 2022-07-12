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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.metrics.kafka.HasCount;
import org.astraea.app.metrics.kafka.KafkaMetrics;

public class MoveCost implements HasClusterCost, HasBrokerCost {
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
      return new TopicPartitionReplica(
          topicPartition.topic(), topicPartition.partition(), brokerSource);
    }
  }

  static final Double MINDISKFREESPACE = 0.2;
  static final String UNKNOWN = "unknown";
  Collection<ReplicaMigrateInfo> distributionChange;
  Collection<ReplicaMigrateInfo> migratedReplicas;
  Map<TopicPartitionReplica, Double> replicaDataRate;
  Map<TopicPartitionReplica, Long> replicaSize;
  Map<Integer, Double> brokerBytesInPerSec;
  Map<Integer, Double> brokerBytesOutPerSec;
  Map<Integer, Double> replicationBytesInPerSec;
  Map<Integer, Double> replicationBytesOutPerSec;
  Map<Integer, Double> availableMigrateBandwidth;
  ReplicaSizeCost replicaSizeCost;
  ReplicaDiskInCost replicaDiskInCost;

  private static boolean filterBean(HasBeanObject hasBeanObject, String metricName) {
    var beanObject = hasBeanObject.beanObject();
    return beanObject != null
        && beanObject.getProperties().containsKey("name")
        && beanObject.getProperties().get("name").equals(metricName);
  }

  private static Map<Integer, Double> brokerTrafficMetrics(
      ClusterInfo clusterInfo, String metricName, Duration sampleWindow) {
    return clusterInfo.clusterBean().all().entrySet().stream()
        .map(
            brokerMetrics -> {
              var sizeTimeSeries =
                  brokerMetrics.getValue().stream()
                      .filter(hasBeanObject -> filterBean(hasBeanObject, metricName))
                      .map(hasBeanObject -> (HasCount) hasBeanObject)
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
                  ((double) (latestSize.count() - windowSize.count()))
                      / ((double) (latestSize.createdTimestamp() - windowSize.createdTimestamp())
                          / 1000)
                      / 1024.0
                      / 1024.0;
              if (latestSize == windowSize) dataRate = 0;
              return Map.entry(brokerMetrics.getKey(), dataRate);
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  boolean checkBrokerInTraffic() {
    // TODO: need replicaOutRate and estimated migrate traffic.
    var brokerBandwidthCap = replicaDiskInCost.brokerBandwidthCap;
    AtomicReference<Boolean> overflow = new AtomicReference<>(false);
    var replicaDataRate = this.replicaDataRate;
    var migratedReplicas = this.migratedReplicas;
    var brokerByteIn = this.brokerBytesInPerSec;
    var brokerByteOut = this.brokerBytesOutPerSec;
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
    brokerBandwidthCap.forEach(
        (broker, bandwidth) -> {
          if (brokerByteIn.get(broker) + brokerTrafficChange.get(broker) > bandwidth)
            overflow.set(true);
        });
    return overflow.get();
  }

  boolean checkFolderSize() {
    AtomicReference<Boolean> overflow = new AtomicReference<>(false);
    var totalBrokerCapacity = replicaSizeCost.totalBrokerCapacity;
    var replicaSize = this.replicaSize;
    var distributionChange = this.distributionChange;
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
                  if ((replicaSizeCost.totalReplicaSizeInPath.get(broker).getOrDefault(path, 0L)
                              + pathSizeChange.getOrDefault(Map.entry(broker, path), 0L))
                          / 1024.0
                          / 1024.0
                          / size
                      > MINDISKFREESPACE) overflow.set(true);
                }));
    return overflow.get();
  }

  boolean checkMigrateSpeed() {
    // TODO: Estimate the traffic available to a replica
    AtomicReference<Boolean> overflow = new AtomicReference<>(false);
    return overflow.get();
  }

  double countMigrateCost() {
    var brokerMigrateInSize =
        migratedReplicas.stream()
            .map(
                replicaMigrateInfo ->
                    Map.entry(
                        replicaMigrateInfo.brokerSink,
                        replicaSize.get(replicaMigrateInfo.sourceTPR())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));
    var brokerScore =
        brokerMigrateInSize.entrySet().stream()
            .map(
                e -> {
                  var y =
                      replicaSizeCost.totalReplicaSizeInPath.get(e.getKey()).values().stream()
                          .mapToLong(x -> x)
                          .sum();
                  return Map.entry(e.getKey(), e.getValue() / 1.0 / y);
                })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return brokerScore.values().stream().mapToDouble(x -> x / brokerScore.size()).sum();
  }

  @Override
  public Fetcher fetcher() {
    return Fetcher.of(
        List.of(
            client ->
                List.of(
                    KafkaMetrics.BrokerTopic.BytesInPerSec.fetch(client),
                    KafkaMetrics.BrokerTopic.BytesOutPerSec.fetch(client),
                    KafkaMetrics.BrokerTopic.ReplicationBytesInPerSec.fetch(client),
                    KafkaMetrics.BrokerTopic.ReplicationBytesOutPerSec.fetch(client)),
            KafkaMetrics.TopicPartition.Size::fetch));
  }

  public MoveCost(ReplicaSizeCost replicaSizeCost, ReplicaDiskInCost replicaDiskInCost) {
    this.replicaSizeCost = replicaSizeCost;
    this.replicaDiskInCost = replicaDiskInCost;
  }

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
    return null;
  }

  @Override
  public ClusterCost clusterCost(
      ClusterInfo clusterInfo, ClusterLogAllocation clusterLogAllocation) {
    var duration = Duration.ofSeconds(10);
    distributionChange = getMigrateReplicas(clusterInfo, clusterLogAllocation, false);
    migratedReplicas = getMigrateReplicas(clusterInfo, clusterLogAllocation, true);
    replicaSize = replicaSizeCost.getReplicaSize(clusterInfo);
    replicaDataRate = replicaDiskInCost.replicaDataRate(clusterInfo, duration);
    brokerBytesInPerSec = brokerTrafficMetrics(clusterInfo, "BytesInPerSec", duration);
    brokerBytesOutPerSec = brokerTrafficMetrics(clusterInfo, "BytesOutPerSec", duration);
    replicationBytesInPerSec =
        brokerTrafficMetrics(clusterInfo, "ReplicationBytesInPerSec", duration);
    replicationBytesOutPerSec =
        brokerTrafficMetrics(clusterInfo, "ReplicationBytesOutPerSec", duration);
    availableMigrateBandwidth =
        replicaDiskInCost.brokerBandwidthCap.entrySet().stream()
            .map(
                brokerBandWidth ->
                    Map.entry(
                        brokerBandWidth.getKey(),
                        brokerBandWidth.getValue()
                            - brokerBytesInPerSec.get(brokerBandWidth.getKey())
                            - replicationBytesInPerSec.get(brokerBandWidth.getKey())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    checkFolderSize();
    checkBrokerInTraffic();
    checkMigrateSpeed();
    var migrateCost = countMigrateCost();
    return () -> migrateCost;
  }

  public boolean overflow() {
    return checkBrokerInTraffic() && checkFolderSize() & checkMigrateSpeed();
  }

  public List<String> migrateSize() {
    return null;
  }

  public List<String> EstimatedMigrateTime() {
    return null;
  }

  public List<ReplicaMigrateInfo> getMigrateReplicas(
      ClusterInfo clusterInfo, ClusterLogAllocation clusterLogAllocation, boolean fromLeader) {
    var leaderReplicas =
        clusterInfo.topics().stream()
            .map(clusterInfo::availableReplicaLeaders)
            .flatMap(
                replicaInfos ->
                    replicaInfos.stream()
                        .map(
                            replicaInfo ->
                                Map.entry(
                                    new TopicPartition(
                                        replicaInfo.topic(), replicaInfo.partition()),
                                    Map.entry(
                                        TopicPartitionReplica.of(
                                            replicaInfo.topic(),
                                            replicaInfo.partition(),
                                            replicaInfo.nodeInfo().id()),
                                        replicaInfo.dataFolder()))))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var beforeMigrate =
        clusterInfo.topics().stream()
            .map(clusterInfo::availableReplicas)
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
        clusterLogAllocation
            .topicPartitionStream()
            .flatMap(
                tp ->
                    clusterLogAllocation.logPlacements(tp).stream()
                        .map(
                            logPlacement ->
                                Map.entry(
                                    TopicPartitionReplica.of(
                                        tp.topic(), tp.partition(), logPlacement.broker()),
                                    logPlacement.logDirectory().orElse(UNKNOWN))))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    if (fromLeader)
      return afterMigrate.entrySet().stream()
          .filter(newTPR -> !beforeMigrate.containsKey(newTPR.getKey()))
          .map(
              newTPR -> {
                var tp = new TopicPartition(newTPR.getKey().topic(), newTPR.getKey().partition());
                var sinkBroker = newTPR.getKey().brokerId();
                return new ReplicaMigrateInfo(
                    tp,
                    leaderReplicas.get(tp).getKey().brokerId(),
                    sinkBroker,
                    leaderReplicas.get(tp).getValue().orElse(UNKNOWN),
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
                          new TopicPartition(oldTPR.getKey().topic(), oldTPR.getKey().partition()),
                          Map.of(oldTPR.getKey().brokerId(), oldTPR.getValue())))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      var sinkChange =
          afterMigrate.entrySet().stream()
              .filter(newTPR -> !beforeMigrate.containsKey(newTPR.getKey()))
              .map(
                  newTPR ->
                      Map.entry(
                          new TopicPartition(newTPR.getKey().topic(), newTPR.getKey().partition()),
                          Map.of(newTPR.getKey().brokerId(), newTPR.getValue())))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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
