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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.ReplicaInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.metrics.KafkaMetrics;
import org.astraea.app.metrics.broker.HasValue;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.partitioner.Configuration;

/**
 * The result is computed by "Size.Value". "Size.Value" responds to the replica log size of brokers.
 * The calculation method of the score is the replica log usage space divided by the available space
 * on the hard disk
 */
public class ReplicaSizeCost implements HasBrokerCost, HasPartitionCost, HasClusterCost {
  static final String BROKERCAPACITY = "brokerCapacityConfig";
  Map<Integer, Map<String, Integer>> totalBrokerCapacity;
  Map<Integer, Map<String, Long>> totalReplicaSizeInPath;

  public ReplicaSizeCost(Configuration configuration) {
    this.totalBrokerCapacity = convert(configuration.requireString(BROKERCAPACITY));
  }

  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.of(KafkaMetrics.TopicPartition.Size::fetch);
  }

  /**
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @return a BrokerCost contains the ratio of the used space to the free space of each broker
   */
  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
      var sizeOfPartition = getReplicaSize(clusterInfo);
      totalReplicaSizeInPath =
              clusterInfo.topics().stream()
                      .flatMap(
                              topic ->
                                      clusterInfo.replicas(topic).stream()
                                              .map(
                                                      replicaInfo ->
                                                              Map.entry(
                                                                      replicaInfo.nodeInfo().id(),
                                                                      Map.of(
                                                                              replicaInfo.dataFolder().orElseThrow(),
                                                                              sizeOfPartition.get(
                                                                                      new TopicPartition(
                                                                                              replicaInfo.topic(),
                                                                                              replicaInfo.partition()
                                                                                      ))
                                                                      ))))
                      .collect(
                              Collectors.toMap(
                                      Map.Entry::getKey,
                                      Map.Entry::getValue,
                                      (x1, x2) -> {
                                          var path = x2.keySet().iterator().next();
                                          var map = new HashMap<String, Long>();
                                          map.putAll(x1);
                                          map.putAll(x2);
                                          if (x1.containsKey(path)) map.put(path, x1.get(path) + x2.get(path));
                                          return map;
                                      }));
      checkBrokerPath(totalReplicaSizeInPath);
    totalReplicaSizeInPath =
        clusterInfo.topics().stream()
            .flatMap(
                topic ->
                    clusterInfo.replicas(topic).stream()
                        .map(
                            replicaInfo ->
                                Map.entry(
                                    replicaInfo.nodeInfo().id(),
                                    Map.of(
                                        replicaInfo.dataFolder().orElseThrow(),
                                        sizeOfPartition.get(
                                            new TopicPartition(
                                                replicaInfo.topic(),
                                                replicaInfo.partition()
                                               ))
                                    ))))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (x1, x2) -> {
                      var path = x2.keySet().iterator().next();
                      var map = new HashMap<String, Long>();
                      map.putAll(x1);
                      map.putAll(x2);
                      if (x1.containsKey(path)) map.put(path, x1.get(path) + x2.get(path));
                      return map;
                    }));
    checkBrokerPath(totalReplicaSizeInPath);
    var brokerSizeScore = brokerScore();

    return () -> brokerSizeScore;
  }

  /**
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @return a BrokerCost contains the ratio of the used space to the available space of replicas in
   *     each broker
   */
  @Override
  public PartitionCost partitionCost(ClusterInfo clusterInfo) {
    final long ONEMEGA = Math.round(Math.pow(2, 20));
    var sizeOfPartition = getReplicaSize(clusterInfo);
    TreeMap<TopicPartitionReplica, Double> replicaCost =
        new TreeMap<>(
            Comparator.comparing(TopicPartitionReplica::brokerId)
                .thenComparing(TopicPartitionReplica::topic)
                .thenComparing(TopicPartitionReplica::partition));
      clusterInfo.clusterBean().mapByReplica().keySet().forEach((tpr) ->
              replicaCost.put(tpr, sizeOfPartition.get(new TopicPartition(tpr.topic(),tpr.partition()))/1.0));

    var scoreForTopic =
        clusterInfo.topics().stream()
            .map(
                topic ->
                    Map.entry(
                        topic,
                        clusterInfo.replicas(topic).stream()
                            .filter(ReplicaInfo::isLeader)
                            .map(
                                partitionInfo ->
                                    new TopicPartitionReplica(
                                        partitionInfo.topic(),
                                        partitionInfo.partition(),
                                        partitionInfo.nodeInfo().id()))
                            .map(
                                tpr -> {
                                  var score =
                                      replicaCost.entrySet().stream()
                                          .filter(
                                              x ->
                                                  x.getKey().topic().equals(tpr.topic())
                                                      && (x.getKey().partition()
                                                          == tpr.partition()))
                                          .mapToDouble(Map.Entry::getValue)
                                          .max()
                                          .orElseThrow(
                                              () ->
                                                  new IllegalStateException(
                                                      tpr + " topic/partition size not found"));
                                  if (score > 1) score = 1.0;
                                  return Map.entry(
                                      new TopicPartition(tpr.topic(), tpr.partition()), score);
                                })
                            .collect(
                                Collectors.toUnmodifiableMap(
                                    Map.Entry::getKey, Map.Entry::getValue))))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    var scoreForBroker =
        clusterInfo.nodes().stream()
            .map(
                node ->
                    Map.entry(
                        node.id(),
                        replicaCost.entrySet().stream()
                            .filter((tprScore) -> tprScore.getKey().brokerId() == node.id())
                            .collect(
                                Collectors.toMap(
                                    x ->
                                        new TopicPartition(
                                            x.getKey().topic(), x.getKey().partition()),
                                    Map.Entry::getValue))))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    return new PartitionCost() {

      @Override
      public Map<TopicPartition, Double> value(String topic) {
        return scoreForTopic.get(topic);
      }

      @Override
      public Map<TopicPartition, Double> value(int brokerId) {
        return scoreForBroker.get(brokerId);
      }
    };
  }

  void checkBrokerPath(Map<Integer, Map<String, Long>> totalReplicaSizeInPath) {
    totalReplicaSizeInPath.forEach(
        (key, value) -> {
          if (!totalBrokerCapacity.get(key).keySet().containsAll(value.keySet()))
            throw new IllegalArgumentException(
                "Path is not mount at broker ,check properties file");
        });
  }

  /**
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @return a map contain the replica log size of each topic/partition
   */
  public Map<TopicPartition, Long> getReplicaSize(ClusterInfo clusterInfo) {
    return clusterInfo.clusterBean().mapByPartition().entrySet().stream()
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

  public Map<Integer, Double> brokerScore() {
    return totalReplicaSizeInPath.entrySet().stream()
        .map(
            brokerPath ->
                Map.entry(
                    brokerPath.getKey(),
                    brokerPath.getValue().entrySet().stream()
                        .mapToDouble(
                            x ->
                                x.getValue()
                                    / 1024.0
                                    / 1024.0
                                    / totalBrokerCapacity.get(brokerPath.getKey()).get(x.getKey())
                                    / totalBrokerCapacity.get(brokerPath.getKey()).size())
                        .sum()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public ClusterCost clusterCost(
      ClusterInfo clusterInfo) {
      var sizeOfPartition = getReplicaSize(clusterInfo);
      totalReplicaSizeInPath =
              clusterInfo.topics().stream()
                      .flatMap(
                              topic ->
                                      clusterInfo.replicas(topic).stream()
                                              .map(
                                                      replicaInfo ->
                                                              Map.entry(
                                                                      replicaInfo.nodeInfo().id(),
                                                                      Map.of(
                                                                              replicaInfo.dataFolder().orElseThrow(),
                                                                              sizeOfPartition.get(
                                                                                      new TopicPartition(
                                                                                              replicaInfo.topic(),
                                                                                              replicaInfo.partition()
                                                                                      ))
                                                                      ))))
                      .collect(
                              Collectors.toMap(
                                      Map.Entry::getKey,
                                      Map.Entry::getValue,
                                      (x1, x2) -> {
                                          var path = x2.keySet().iterator().next();
                                          var map = new HashMap<String, Long>();
                                          map.putAll(x1);
                                          map.putAll(x2);
                                          if (x1.containsKey(path)) map.put(path, x1.get(path) + x2.get(path));
                                          return map;
                                      }));
    var brokerSizeScore = brokerScore();
    var mean = brokerSizeScore.values().stream().mapToDouble(x -> x).sum() / brokerSizeScore.size();
    var sd =
        Math.sqrt(brokerSizeScore.values().stream().mapToDouble(
                score ->
                        Math.pow((score - mean),2)).sum()
            / brokerSizeScore.size());
    return () -> sd;
  }

  static Map.Entry<Integer, String> transformEntry(String entry) {
    Map<String, Integer> brokerPath = new HashMap<>();
    final Pattern serviceUrlKeyPattern =
        Pattern.compile("broker\\.(?<brokerId>[1-9][0-9]{0,9})\\.(?<path>/.{0,50})");
    final Matcher matcher = serviceUrlKeyPattern.matcher(entry);
    if (matcher.matches()) {
      try {
        int brokerId = Integer.parseInt(matcher.group("brokerId"));
        var path = matcher.group("path");
        return Map.entry(brokerId, path);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Bad integer format for " + entry, e);
      }
    } else {
      throw new IllegalArgumentException(
          "Bad key format for "
              + entry
              + " no match for the following format :"
              + serviceUrlKeyPattern.pattern());
    }
  }

  public static Map<Integer, Map<String, Integer>> convert(String value) {
    final Properties properties = new Properties();

    try (var reader = Files.newBufferedReader(Path.of(value))) {
      properties.load(reader);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return properties.entrySet().stream()
        .map(entry -> Map.entry((String) entry.getKey(), (String) entry.getValue()))
        .map(
            entry -> {
              var brokerId = transformEntry(entry.getKey()).getKey();
              var path = transformEntry(entry.getKey()).getValue();
              return Map.entry(brokerId, Map.of(path, Integer.parseInt(entry.getValue())));
            })
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (x1, x2) -> {
                  var map = new HashMap<String, Integer>();
                  map.putAll(x1);
                  map.putAll(x2);
                  return map;
                }));
  }
}
