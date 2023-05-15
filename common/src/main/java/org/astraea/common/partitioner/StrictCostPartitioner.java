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
package org.astraea.common.partitioner;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.BrokerTopic;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.cost.BrokerCost;
import org.astraea.common.cost.CostFunction;
import org.astraea.common.cost.HasBrokerCost;
import org.astraea.common.cost.HasPartitionCost;
import org.astraea.common.cost.NoSufficientMetricsException;
import org.astraea.common.cost.NodeLatencyCost;
import org.astraea.common.cost.ReplicaLeaderSizeCost;
import org.astraea.common.metrics.JndiClient;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.collector.MetricStore;

/**
 * this partitioner scores the nodes by multiples cost functions. Each function evaluate the target
 * node by different metrics. The default cost function ranks nodes by request latency. It means the
 * node having lower request latency get higher score. After determining the node, this partitioner
 * scores the partitions with partition cost function. The default partition cost function ranks
 * partition by partition size. It means the node having lower size get higher score.
 *
 * <p>The important config is JMX port. Most cost functions need the JMX metrics to score nodes.
 * Normally, all brokers use the same JMX port, so you can just define the `jmx.port=12345`. If one
 * of brokers uses different JMX client port, you can define `broker.1000.jmx.port=11111` (`1000` is
 * the broker id) to replace the value of `jmx.port`. If the jmx port is undefined, only local mbean
 * client is created for each cost function
 *
 * <p>You can configure the cost functions you want to use. By giving the name of that cost function
 * and its weight. For example,
 * `org.astraea.cost.ThroughputCost=1,org.astraea.cost.broker.BrokerOutputCost=1`.
 */
public class StrictCostPartitioner extends Partitioner {
  static final int ROUND_ROBIN_LENGTH = 400;
  static final String JMX_PORT = "jmx.port";
  static final String ROUND_ROBIN_LEASE_KEY = "round.robin.lease";
  // visible for testing
  MetricStore metricStore = null;

  private Duration roundRobinLease = Duration.ofSeconds(4);
  HasBrokerCost brokerCost = new NodeLatencyCost();
  HasPartitionCost partitionCost = new ReplicaLeaderSizeCost();
  // The minimum partition cost of every topic of every broker.
  final Map<String, Map<Integer, Integer>> minPartition = new HashMap<>();
  long partitionUpdateTime = 0L;
  Function<Integer, Integer> jmxPortGetter =
      (id) -> {
        throw new NoSuchElementException("must define either broker.x.jmx.port or jmx.port");
      };
  RoundRobinKeeper roundRobinKeeper;

  @Override
  public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
    var partitionLeaders = clusterInfo.replicaLeaders(topic);
    // just return first partition if there is no available partitions
    if (partitionLeaders.isEmpty()) return 0;

    // just return the only one available partition
    if (partitionLeaders.size() == 1) return partitionLeaders.get(0).partition();

    try {
      roundRobinKeeper.tryToUpdate(
          clusterInfo,
          () -> costToScore(brokerCost.brokerCost(clusterInfo, metricStore.clusterBean())));
    } catch (NoSufficientMetricsException e) {
      // There is not enough metrics for the cost functions computing teh broker cost. We should not
      // update the round-robin keeper. Reuse the weights that were kept in the round-robin keeper.

      // Let the user know the cost-functions were complaining.
      e.printStackTrace();
    }

    var target = roundRobinKeeper.next();

    // Choose a preferred partition from candidate by partition cost function
    var preferredPartition =
        tryUpdateMinPartition(
            topic,
            target,
            (tp, id) -> {
              // Update the preferred partition according to the topic and target broker id
              // The target broker id may be determined previously by broker cost
              // The returned value may be a special value "-1" which represents no preferred
              // partition.
              // There are three conditions that the special value "-1" appears:
              //   1. the target broker id is not valid
              //   2. the target broker id has no partition leader
              //   3. no partition cost in the target broker id
              if (id == -1) return -1;
              var candidate = clusterInfo.replicaLeaders(BrokerTopic.of(target, topic));
              if (candidate.isEmpty()) return -1;
              var candidateSet =
                  candidate.stream()
                      .map(Replica::topicPartition)
                      .collect(HashSet::new, HashSet::add, HashSet::addAll);
              var preferred =
                  partitionCost
                      .partitionCost(clusterInfo, metricStore.clusterBean())
                      .value()
                      .entrySet()
                      .stream()
                      .filter(e -> candidateSet.contains(e.getKey()))
                      .min(Comparator.comparingDouble(Map.Entry::getValue));

              return preferred.map(e -> e.getKey().partition()).orElse(-1);
            });
    // Check if we can get preferred partition from partition cost function
    if (preferredPartition != -1) return preferredPartition;

    // TODO: if the topic partitions are existent in fewer brokers, the target gets -1 in most cases
    // Check "target valid" and the target "has partition leader".
    var candidate =
        target < 0 ? partitionLeaders : clusterInfo.replicaLeaders(BrokerTopic.of(target, topic));
    candidate = candidate.isEmpty() ? partitionLeaders : candidate;
    // Randomly choose from candidate.
    return candidate.get((int) (Math.random() * candidate.size())).partition();
  }

  /**
   * @param topic the topic we send record
   * @param brokerId the broker id that has been determined by the broker cost function
   * @param partition update function
   * @return the cached partition if the update time is not expired; otherwise update the partition
   *     by the given supplier
   */
  private int tryUpdateMinPartition(
      String topic, int brokerId, BiFunction<String, Integer, Integer> partition) {
    synchronized (minPartition) {
      if (Utils.isExpired(partitionUpdateTime, roundRobinLease)) {
        partitionUpdateTime = System.currentTimeMillis();
        minPartition.clear();
      }
      return minPartition
          .computeIfAbsent(topic, (tp) -> new HashMap<>())
          .computeIfAbsent(brokerId, (id) -> partition.apply(topic, id));
    }
  }
  /**
   * The value of cost returned from cost function is conflict to score, since the higher cost
   * represents lower score. This helper reverses the cost by subtracting the cost from "max cost".
   *
   * @param cost to convert
   * @return weights
   */
  static Map<Integer, Double> costToScore(BrokerCost cost) {
    // reduce the both zero and negative number
    var shift =
        cost.value().values().stream()
            .min(Double::compare)
            .map(Math::abs)
            .filter(v -> v > 0)
            .orElse(0.01);
    var max = cost.value().values().stream().max(Double::compare);
    return max.map(
            m ->
                cost.value().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> m - e.getValue() + shift)))
        .orElse(cost.value());
  }

  @Override
  public void configure(Configuration config) {
    var configuredFunctions =
        Utils.costFunctions(
            config.filteredPrefixConfigs(COST_PREFIX).raw(), CostFunction.class, config);
    if (!configuredFunctions.isEmpty()) {
      this.brokerCost =
          HasBrokerCost.of(
              configuredFunctions.entrySet().stream()
                  .filter(e -> e.getKey() instanceof HasBrokerCost)
                  .collect(
                      Collectors.toUnmodifiableMap(
                          e -> (HasBrokerCost) e.getKey(), Map.Entry::getValue)));
      this.partitionCost =
          HasPartitionCost.of(
              configuredFunctions.entrySet().stream()
                  .filter(e -> e.getKey() instanceof HasPartitionCost)
                  .collect(
                      Collectors.toUnmodifiableMap(
                          e -> (HasPartitionCost) e.getKey(), Map.Entry::getValue)));
    }
    var customJmxPort = PartitionerUtils.parseIdJMXPort(config);
    var defaultJmxPort = config.integer(JMX_PORT);
    this.jmxPortGetter =
        id ->
            Optional.ofNullable(customJmxPort.get(id))
                .or(() -> defaultJmxPort)
                .orElseThrow(
                    () -> new NoSuchElementException("failed to get jmx port for broker: " + id));
    config
        .string(ROUND_ROBIN_LEASE_KEY)
        .map(Utils::toDuration)
        .ifPresent(d -> this.roundRobinLease = d);
    Supplier<CompletionStage<Map<Integer, MBeanClient>>> clientSupplier =
        () ->
            admin
                .brokers()
                .thenApply(
                    brokers -> {
                      var map = new HashMap<Integer, JndiClient>();
                      brokers.forEach(
                          b ->
                              map.put(
                                  b.id(), JndiClient.of(b.host(), jmxPortGetter.apply(b.id()))));
                      // add local client to fetch consumer metrics
                      map.put(-1, JndiClient.local());
                      return Collections.unmodifiableMap(map);
                    });

    metricStore =
        MetricStore.builder()
            .receivers(List.of(MetricStore.Receiver.local(clientSupplier)))
            .sensorsSupplier(() -> Map.of(this.brokerCost.metricSensor(), (integer, e) -> {}))
            .build();
    this.roundRobinKeeper = RoundRobinKeeper.of(ROUND_ROBIN_LENGTH, roundRobinLease);
  }

  @Override
  public void close() {
    metricStore.close();
    super.close();
  }
}
