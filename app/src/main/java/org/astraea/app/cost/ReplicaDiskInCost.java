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
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.KafkaMetrics;
import org.astraea.app.metrics.broker.HasValue;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.partitioner.Configuration;

/**
 * The result is computed by "Size.Value" ,and createdTimestamp in the metrics. "Size.Value"
 * responds to the replica log size of brokers. The calculation method of the score is the rate of
 * increase of log size per unit time divided by the upper limit of broker bandwidth.
 */
public class ReplicaDiskInCost implements HasBrokerCost, HasPartitionCost, HasClusterCost {
  static final String BROKERBANDWIDTH = "brokerBandwidthConfig";
  Map<Integer, Double> brokerLoad;

  public ReplicaDiskInCost(Configuration configuration) {
  }

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
    final Map<Integer, List<TopicPartitionReplica>> topicPartitionOfEachBroker =
        clusterInfo.topics().stream()
            .flatMap(topic -> clusterInfo.replicas(topic).stream())
            .map(
                replica ->
                    new TopicPartitionReplica(
                        replica.topic(), replica.partition(), replica.nodeInfo().id()))
            .collect(Collectors.groupingBy(TopicPartitionReplica::brokerId));
    final var actual =
        clusterInfo.nodes().stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    NodeInfo::id,
                    node -> topicPartitionOfEachBroker.getOrDefault(node.id(), List.of())));
    final var topicPartitionDataRate = topicPartitionDataRate(clusterInfo, Duration.ofSeconds(3));
    var brokerSizeScore =
        actual.entrySet().stream()
            .map(
                entry ->
                    Map.entry(
                        entry.getKey(),
                        entry.getValue().stream()
                            .mapToDouble(
                                x ->
                                    topicPartitionDataRate.getOrDefault(
                                        new TopicPartition(x.topic(), x.partition()), 0.0))
                            .sum()))
            .map(
                entry ->
                    Map.entry(
                        entry.getKey(), entry.getValue()))
            .map(entry -> Map.entry(entry.getKey(), Math.min(entry.getValue(), 1)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var mean = brokerSizeScore.values().stream().mapToDouble(x -> x).sum() / brokerSizeScore.size();
    var sd =
        Math.sqrt(
            brokerSizeScore.values().stream()
                    .mapToDouble(score -> Math.pow((score - mean), 2))
                    .sum()
                / brokerSizeScore.size());
    return () -> brokerSizeScore;
  }

  @Override
  public PartitionCost partitionCost(ClusterInfo clusterInfo) {
    var replicaIn = replicaDataRate(clusterInfo, Duration.ofSeconds(2));
    var replicaScore =
        new TreeMap<TopicPartitionReplica, Double>(
            Comparator.comparing(TopicPartitionReplica::brokerId)
                .thenComparing(TopicPartitionReplica::topic)
                .thenComparing(TopicPartitionReplica::partition));
    replicaIn.forEach(
        (tpr, rate) -> {
          var score = rate;
          if (score >= 1) score = 1.0;
          replicaScore.put(tpr, score);
        });
    var scoreForTopic =
        clusterInfo.topics().stream()
            .map(
                topic ->
                    Map.entry(
                        topic,
                        replicaScore.entrySet().stream()
                            .filter(x -> x.getKey().topic().equals(topic))
                            .collect(
                                Collectors.groupingBy(
                                    x ->
                                        new TopicPartition(
                                            x.getKey().topic(), x.getKey().partition())))
                            .entrySet()
                            .stream()
                            .map(
                                entry ->
                                    Map.entry(
                                        entry.getKey(),
                                        entry.getValue().stream()
                                            .mapToDouble(Map.Entry::getValue)
                                            .max()
                                            .orElseThrow()))
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
                        replicaScore.entrySet().stream()
                            .filter(x -> x.getKey().brokerId() == node.id())
                            .collect(
                                Collectors.groupingBy(
                                    x ->
                                        new TopicPartition(
                                            x.getKey().topic(), x.getKey().partition())))
                            .entrySet()
                            .stream()
                            .map(
                                entry ->
                                    Map.entry(
                                        entry.getKey(),
                                        entry.getValue().stream()
                                            .mapToDouble(Map.Entry::getValue)
                                            .max()
                                            .orElseThrow()))
                            .collect(
                                Collectors.toUnmodifiableMap(
                                    Map.Entry::getKey, Map.Entry::getValue))))
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

  @Override
  public ClusterCost clusterCost(ClusterInfo clusterInfo) {
    var duration = Duration.ofSeconds(20);

    final Map<Integer, List<TopicPartitionReplica>> topicPartitionOfEachBroker =
        clusterInfo.topics().stream()
            .flatMap(topic -> clusterInfo.replicas(topic).stream())
            .map(
                replica ->
                    new TopicPartitionReplica(
                        replica.topic(), replica.partition(), replica.nodeInfo().id()))
            .collect(Collectors.groupingBy(TopicPartitionReplica::brokerId));
    final var topicPartitionDataRate = topicPartitionDataRate(clusterInfo, duration);

    var brokerDataRate =
        topicPartitionOfEachBroker.entrySet().stream()
            .map(
                entry ->
                    Map.entry(
                        entry.getKey(),
                        entry.getValue().stream()
                            .mapToDouble(
                                x ->
                                    topicPartitionDataRate.getOrDefault(
                                        new TopicPartition(x.topic(), x.partition()), 0.0))
                            .sum()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var dataRateMean =
        brokerDataRate.values().stream().mapToDouble(x -> x).sum() / brokerDataRate.size();
    var dataRateSD =
        Math.sqrt(
            brokerDataRate.values().stream()
                    .mapToDouble(score -> Math.pow((score - dataRateMean), 2))
                    .sum()
                / brokerDataRate.size());
    /*
    var tScore =
        brokerDataRate.entrySet().stream()
            .map(
                x ->
                    Map.entry(
                        x.getKey(), (((x.getValue() - dataRateMean) / dataRateSD) * 10 + 50) / 100))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var sortedScore = tScore.values().stream().sorted().collect(Collectors.toList());
    if (sortedScore.size() >= 2)
      return () -> sortedScore.get(sortedScore.size() - 1) - sortedScore.get(0);

     */
    var cv=dataRateSD/dataRateMean;
    if (cv>1)
      return ()->1.0;
    return () -> cv;
  }

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.of(KafkaMetrics.TopicPartition.Size::fetch);
  }

  /**
   * Calculate the maximum increase rate of each topic/partition, across the whole cluster.
   *
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @param sampleWindow the time interval for calculating the data rate, noted that if the metrics
   *     doesn't have the sufficient old metric then an exception will likely be thrown.
   * @return a map contain the maximum increase rate of each topic/partition log
   */
  public Map<TopicPartition, Double> topicPartitionDataRate(
      ClusterInfo clusterInfo, Duration sampleWindow) {
    return replicaDataRate(clusterInfo, sampleWindow).entrySet().stream()
        .map(
            x -> {
              var tpr = x.getKey();
              return Map.entry(new TopicPartition(tpr.topic(), tpr.partition()), x.getValue());
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x1, x2) -> x1));
  }

  public Map<TopicPartitionReplica, Double> replicaDataRate(
      ClusterInfo clusterInfo, Duration sampleWindow) {
    return clusterInfo.clusterBean().mapByReplica().entrySet().stream()
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
                              sampleWindow.toMillis()
                                  > latestSize.createdTimestamp() - bean.createdTimestamp())
                      .findFirst()
                      .orElseThrow(()->new IllegalArgumentException("metrics:"+clusterInfo.clusterBean().all().values().size()));
              var dataRate =
                  ((double) (latestSize.value() - windowSize.value()))
                      / ((double) (latestSize.createdTimestamp() - windowSize.createdTimestamp())
                          / 1000)
                      / 1024.0
                      / 1024.0;
              return Map.entry(metrics.getKey(), dataRate);
            })
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  static Map.Entry<Integer, Integer> transformEntry(Map.Entry<String, String> entry) {
    final Pattern serviceUrlKeyPattern = Pattern.compile("broker\\.(?<brokerId>[0-9]{1,9})");
    final Matcher matcher = serviceUrlKeyPattern.matcher(entry.getKey());
    if (matcher.matches()) {
      try {
        int brokerId = Integer.parseInt(matcher.group("brokerId"));
        return Map.entry(brokerId, Integer.parseInt(entry.getValue()));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Bad integer format for " + entry.getKey(), e);
      }
    } else {
      throw new IllegalArgumentException(
          "Bad key format for "
              + entry.getKey()
              + " no match for the following format :"
              + serviceUrlKeyPattern.pattern());
    }
  }

  public static Map<Integer, Integer> convert(String value) {
    final Properties properties = new Properties();

    try (var reader = Files.newBufferedReader(Path.of(value))) {
      properties.load(reader);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return properties.entrySet().stream()
        .map(entry -> Map.entry((String) entry.getKey(), (String) entry.getValue()))
        .map(ReplicaDiskInCost::transformEntry)
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
