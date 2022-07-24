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

import static org.astraea.app.cost.ReplicaDiskInCost.BROKERBANDWIDTH;
import static org.astraea.app.cost.ReplicaSizeCost.BROKERCAPACITY;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.KafkaMetrics;
import org.astraea.app.metrics.broker.HasCount;
import org.astraea.app.metrics.broker.HasValue;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.partitioner.Configuration;

public class MoveCost implements HasMoveCost {
  public MoveCost(Configuration configuration) {
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
  Map<Integer, Double> brokerScore;
  Map<Integer, Map<String, Integer>> totalBrokerCapacity;
  Map<Integer, Integer> brokerBandwidthCap;

  private static boolean filterBean(HasBeanObject hasBeanObject, String metricName) {
    var beanObject = hasBeanObject.beanObject();
    return beanObject != null
        && beanObject.properties().containsKey("name")
        && beanObject.properties().get("name").equals(metricName);
  }

  public static Map<Integer, Double> brokerTrafficMetrics(
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

  boolean checkBrokerInTraffic() {
    // TODO: need replicaOutRate and estimated migrate traffic.
    var brokerBandwidthCap = this.brokerBandwidthCap;
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
          if (brokerByteIn.get(broker) + brokerTrafficChange.getOrDefault(broker, 0.0) > bandwidth)
            overflow.set(true);
        });
    return overflow.get();
  }

  boolean checkFolderSize() {
    AtomicReference<Boolean> overflow = new AtomicReference<>(false);
    var totalBrokerCapacity = this.totalBrokerCapacity;
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
                  if ((totalBrokerCapacity.get(broker).getOrDefault(path, 0)
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
    return brokerScore.values().stream().mapToDouble(x -> x / brokerScore.size()).sum();
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
      ClusterInfo clusterInfo, ClusterLogAllocation clusterLogAllocation) {
    var duration = Duration.ofSeconds(20);
    distributionChange = getMigrateReplicas(clusterInfo, clusterLogAllocation, false);
    migratedReplicas = getMigrateReplicas(clusterInfo, clusterLogAllocation, true);
    replicaSize = getReplicaSize(clusterInfo);
    replicaDataRate = replicaDataRate(clusterInfo, duration);
    brokerBytesInPerSec = brokerTrafficMetrics(clusterInfo, "BytesInPerSec", duration);
    brokerBytesOutPerSec = brokerTrafficMetrics(clusterInfo, "BytesOutPerSec", duration);
    replicationBytesInPerSec =
        brokerTrafficMetrics(clusterInfo, "ReplicationBytesInPerSec", duration);
    replicationBytesOutPerSec =
        brokerTrafficMetrics(clusterInfo, "ReplicationBytesOutPerSec", duration);
    availableMigrateBandwidth =
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
                        replicaSize.get(replicaMigrateInfo.sourceTPR())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));
    var trafficSeries =
            migratedReplicas
                    .stream()
                    .map(
                            x-> replicaDataRate.get(x.sourceTPR())
                    ).sorted()
                    .filter(x->x!=0.0)
                    .collect(Collectors.toList());
    var meanTrafficSeries = trafficSeries.stream().mapToDouble(x->x).sum()/trafficSeries.size();
    var SDTrafficSeries =
            Math.sqrt(
            trafficSeries.stream()
                    .mapToDouble(score -> Math.pow((score - meanTrafficSeries), 2))
                    .sum()
                    / trafficSeries.size());

    var totalMigrateTraffic = trafficSeries.stream().mapToDouble(x->x).sum();
    var totalReplicaTrafficInSink =
            replicaDataRate
                    .entrySet()
                    .stream()
                    .filter(x->brokerMigrateInSize.containsKey(x.getKey().brokerId()))
                    .mapToDouble(Map.Entry::getValue).sum();
    var meanMigrateSize = trafficSeries.stream().mapToDouble(x->x).sum()/trafficSeries.size();
    var sdMigrateSize =
            Math.sqrt(
            trafficSeries.stream()
                    .mapToDouble(score -> Math.pow((score - meanMigrateSize), 2))
                    .sum()
                    / trafficSeries.size());
      var migrateTrafficRange = 0.0;
    if (trafficSeries.size()>=2) {
        var tScoreMigrateTraffic =
                trafficSeries.stream()
                        .map(
                                x ->
                                        (((x - meanMigrateSize) / sdMigrateSize) * 10 + 50) / 100)
                        .collect(Collectors.toList());
        migrateTrafficRange = (tScoreMigrateTraffic.get(tScoreMigrateTraffic.size() - 1)
                - tScoreMigrateTraffic.stream().findFirst().orElseThrow());
    }else
        migrateTrafficRange =0.0;
    var brokerMigrateScore = tScore(brokerMigrateInSize);

    //var moveNumScore = tScore(brokerMigrateNum);
    brokerScore =
        brokerMigrateInSize.keySet().stream()
            .map(
                    aLong -> {
                        var score = brokerMigrateScore.get(aLong);
                        return Map.entry(aLong, score);
                    })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
     var migrateCost = brokerScore.values().stream().mapToDouble(x -> x / brokerScore.size()).sum();
     var totalMigrateSize = brokerMigrateInSize.values().stream()
             .mapToDouble(x->x /1024.0 /1024.0).sum();
     var sinkBrokerSize =
             migratedReplicas
                     .stream()
                     .mapToDouble(
                             x->
                                     totalBrokerCapacity.get(x.brokerSink).get(x.pathSink) /1024.0 /1024.0
                     ).sum();
      var total =totalBrokerCapacity.values().stream().mapToDouble(x->x.values().stream().mapToDouble(y->y).sum()).sum()/1024.0/1024.0;
      var totalMigrateSizeScore = totalMigrateSize / total > 1 ? 1 : totalMigrateSize / total ;
     var score = SDTrafficSeries;
     if (replicaDataRate.containsValue(-1.0) ) {
         System.out.println("retention");
         return () -> 1.0;
     }
     return () -> score;
  }

  public Map<Integer, Double> tScore(Map<Integer, Long> brokerEntry) {
    var dataRateMean = brokerEntry.values().stream().mapToDouble(x -> x).sum() / brokerEntry.size();
    var dataRateSD =
        Math.sqrt(
            brokerEntry.values().stream()
                    .mapToDouble(score -> Math.pow((score - dataRateMean), 2))
                    .sum()
                / brokerEntry.size());
    return brokerEntry.entrySet().stream()
        .map(
            x ->
                Map.entry(
                    x.getKey(), (((x.getValue() - dataRateMean) / dataRateSD) * 10 + 50) / 100))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public Map<TopicPartitionReplica, Long> getReplicaSize(ClusterInfo clusterInfo) {
    return clusterInfo.clusterBean().mapByReplica().entrySet().stream()
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
      ClusterInfo clusterInfo, Duration sampleWindow) {
    return clusterInfo.clusterBean().mapByReplica().entrySet().parallelStream()
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
                  dataRate = -1.0;
              if (latestSize == windowSize) dataRate = 0.0;
              return Map.entry(metrics.getKey(), dataRate);
            })
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public boolean overflow() {
    if ((checkBrokerInTraffic() && checkFolderSize() & checkMigrateSpeed()) == true) return true;
    return false;
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
                                        replicaInfo.dataFolder().orElse(UNKNOWN)))))
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
                          new TopicPartition(oldTPR.getKey().topic(), oldTPR.getKey().partition()),
                          Map.of(oldTPR.getKey().brokerId(), oldTPR.getValue())))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x1, x2) -> x1));
      var sinkChange =
          afterMigrate.entrySet().stream()
              .filter(newTPR -> !beforeMigrate.containsKey(newTPR.getKey()))
              .map(
                  newTPR ->
                      Map.entry(
                          new TopicPartition(newTPR.getKey().topic(), newTPR.getKey().partition()),
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
