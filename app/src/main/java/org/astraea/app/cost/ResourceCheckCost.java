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
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.broker.HasCount;
import org.astraea.app.metrics.broker.HasGauge;
import org.astraea.app.metrics.broker.LogMetrics;
import org.astraea.app.metrics.broker.ServerMetrics;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.partitioner.Configuration;

public class ResourceCheckCost implements HasMoveCost {
  static final double OVERFLOW_SCORE = 9999.0;
  final Duration duration;
  static final String UNKNOWN = "unknown";
  Map<Integer, Integer> brokerBandwidthCap;

  public ResourceCheckCost(Configuration configuration, ClusterBean clusterBean) {
    this.duration =
        Duration.ofSeconds(Integer.parseInt(configuration.string("metrics.duration").orElse("30")));
    this.brokerBandwidthCap =
        clusterBean.all().keySet().stream()
            .map(brokerID -> Map.entry(brokerID, 1250))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  static class MigrateInfo {
    TopicPartition topicPartition;
    int brokerSource;
    int brokerSink;
    String pathSource;
    String pathSink;

    public MigrateInfo(
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

  boolean overflow(
      ClusterInfo originClusterInfo, ClusterInfo newClusterInfo, ClusterBean clusterBean) {
    var migratedReplicas = getMigrateReplicas(originClusterInfo, newClusterInfo, true);
    var replicaDataRate = replicaDataRate(clusterBean, duration);
    return checkBrokerInTraffic(replicaDataRate, migratedReplicas, clusterBean)
        || checkMigrateSpeed();
  }

  boolean checkBrokerInTraffic(
      Map<TopicPartitionReplica, Double> replicaDataRate,
      List<MigrateInfo> migratedReplicas,
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
        migrateInfo -> {
          // TODO: need replicaOutRate
          var sourceChange = brokerTrafficChange.getOrDefault(migrateInfo.brokerSource, 0.0);
          //    - replicaDataRate.getOrDefault(replicaMigrateInfo.sourceTPR(), 0.0);
          var sinkChange =
              brokerTrafficChange.getOrDefault(migrateInfo.brokerSink, 0.0)
                  + replicaDataRate.getOrDefault(migrateInfo.sourceTPR(), 0.0);
          brokerTrafficChange.put(migrateInfo.brokerSource, sourceChange);
          brokerTrafficChange.put(migrateInfo.brokerSink, sinkChange);
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
                            ServerMetrics.Topic.BYTES_IN_PER_SEC.fetch(client12),
                            ServerMetrics.Topic.BYTES_OUT_PER_SEC.fetch(client12),
                            ServerMetrics.Topic.REPLICATION_BYTES_IN_PER_SEC.fetch(client12),
                            ServerMetrics.Topic.REPLICATION_BYTES_OUT_PER_SEC.fetch(client12)),
                    (Fetcher) LogMetrics.Log.SIZE::fetch)
                .flatMap(f -> f.fetch(client).stream())
                .collect(Collectors.toUnmodifiableList()));
  }

  @Override
  public MoveCost moveCost(
      ClusterInfo originClusterInfo, ClusterInfo newClusterInfo, ClusterBean clusterBean) {
    if (overflow(originClusterInfo, newClusterInfo, clusterBean)) return () -> OVERFLOW_SCORE;
    var migratedReplicas = getMigrateReplicas(originClusterInfo, newClusterInfo, true);
    var replicaDataRate = replicaDataRate(clusterBean, duration);
    var trafficSeries =
        migratedReplicas.stream()
            .map(x -> replicaDataRate.get(x.sourceTPR()))
            .sorted()
            .filter(x -> x != 0.0)
            .collect(Collectors.toList());
    if (replicaDataRate.containsValue(-1.0)) return () -> OVERFLOW_SCORE;
    return () -> Dispersion.correlationCoefficient().calculate(trafficSeries);
  }

  public static List<MigrateInfo> getMigrateReplicas(
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
                return new MigrateInfo(
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
      var change = new ArrayList<MigrateInfo>();
      if (sourceChange.keySet().containsAll(sinkChange.keySet())
          && sourceChange.values().size() == sinkChange.values().size())
        sourceChange.forEach(
            (tp, brokerPathMap) -> {
              var sinkBrokerPaths = new HashMap<>(sinkChange.get(tp));
              brokerPathMap.forEach(
                  (sourceBroker, sourcePath) -> {
                    var sinkBrokerPath = sinkBrokerPaths.entrySet().iterator().next();
                    change.add(
                        new MigrateInfo(
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

  // wait for PR#618 merge
  static Map<Integer, Double> brokerTrafficMetrics(
      ClusterBean clusterBean, String metricName, Duration duration) {
    return clusterBean.all().entrySet().stream()
        .map(brokerMetrics -> dataRate(brokerMetrics, metricName, duration))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  static boolean filterBean(HasBeanObject hasBeanObject, String metricName) {
    var beanObject = hasBeanObject.beanObject();
    return beanObject != null
        && beanObject.properties().containsKey("name")
        && beanObject.properties().get("name").equals(metricName);
  }

  static <T> Map.Entry<T, Double> dataRate(
      Map.Entry<T, Collection<HasBeanObject>> brokerMetrics, String metricName, Duration duration) {
    AtomicBoolean brokerLevel = new AtomicBoolean();
    brokerLevel.set(brokerMetrics.getKey() instanceof Integer);

    var sizeTimeSeries =
        brokerMetrics.getValue().stream()
            .filter(
                hasBeanObject -> {
                  if (brokerLevel.get()) return filterBean(hasBeanObject, metricName);
                  return hasBeanObject instanceof HasGauge
                      && hasBeanObject.beanObject().properties().get("type").equals("Log")
                      && hasBeanObject.beanObject().properties().get("name").equals(metricName);
                })
            .sorted(Comparator.comparingLong(HasBeanObject::createdTimestamp).reversed())
            .collect(Collectors.toUnmodifiableList());

    var latestSize = sizeTimeSeries.stream().findFirst().orElseThrow(NoSuchElementException::new);
    var windowSize =
        sizeTimeSeries.stream()
            .dropWhile(
                bean ->
                    bean.createdTimestamp() > latestSize.createdTimestamp() - duration.toMillis())
            .findFirst()
            .orElseThrow();
    var dataRate = -1.0;
    if (brokerLevel.get())
      dataRate =
          (((HasCount) latestSize).count() - ((HasCount) windowSize).count())
              / ((double) (latestSize.createdTimestamp() - windowSize.createdTimestamp()) / 1000)
              / 1024.0
              / 1024.0;
    else {
      dataRate =
          (((HasGauge) latestSize).value() - ((HasGauge) windowSize).value())
              / ((double) (latestSize.createdTimestamp() - windowSize.createdTimestamp()) / 1000)
              / 1024.0
              / 1024.0;
      if (dataRate < 0 || (latestSize == windowSize)) dataRate = -1.0;
    }
    return Map.entry(brokerMetrics.getKey(), dataRate);
  }

  static Map<TopicPartitionReplica, Double> replicaDataRate(
      ClusterBean clusterBean, Duration duration) {
    return clusterBean.mapByReplica().entrySet().parallelStream()
        .map(metrics -> dataRate(metrics, "Size", duration))
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
