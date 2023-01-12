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

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.Lazy;
import org.astraea.common.Utils;
import org.astraea.common.admin.BrokerTopic;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.cost.BrokerCost;
import org.astraea.common.cost.HasBrokerCost;
import org.astraea.common.cost.NodeLatencyCost;
import org.astraea.common.metrics.collector.MetricCollector;

/**
 * this dispatcher scores the nodes by multiples cost functions. Each function evaluate the target
 * node by different metrics. The default cost function ranks nodes by replica leader. It means the
 * node having lower replica leaders get higher score.
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
public class StrictCostDispatcher extends Dispatcher {
  static final int ROUND_ROBIN_LENGTH = 400;
  static final String JMX_PORT = "jmx.port";
  static final String ROUND_ROBIN_LEASE_KEY = "round.robin.lease";
  // visible for testing
  final MetricCollector metricCollector =
      MetricCollector.builder().interval(Duration.ofMillis(1500)).build();

  private Duration roundRobinLease = Duration.ofSeconds(4);
  HasBrokerCost costFunction = new NodeLatencyCost();
  Function<Integer, Optional<Integer>> jmxPortGetter = (id) -> Optional.empty();
  RoundRobinKeeper roundRobinKeeper;

  void tryToUpdateFetcher(ClusterInfo clusterInfo) {
    // register new nodes to metric collector
    costFunction
        .fetcher()
        .ifPresent(
            fetcher ->
                clusterInfo
                    .nodes()
                    .forEach(
                        node -> {
                          if (!metricCollector.listIdentities().contains(node.id())) {
                            jmxPortGetter
                                .apply(node.id())
                                .ifPresent(
                                    port ->
                                        metricCollector.registerJmx(
                                            node.id(),
                                            InetSocketAddress.createUnresolved(node.host(), port)));
                          }
                        }));
  }

  @Override
  public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
    var partitionLeaders = clusterInfo.replicaLeaders(topic);
    // just return first partition if there is no available partitions
    if (partitionLeaders.isEmpty()) return 0;

    // just return the only one available partition
    if (partitionLeaders.size() == 1) return partitionLeaders.get(0).partition();

    tryToUpdateFetcher(clusterInfo);

    roundRobinKeeper.tryToUpdate(
        clusterInfo,
        Lazy.of(
            () ->
                costToScore(costFunction.brokerCost(clusterInfo, metricCollector.clusterBean()))));

    var target = roundRobinKeeper.next();

    // TODO: if the topic partitions are existent in fewer brokers, the target gets -1 in most cases
    var candidate =
        target < 0 ? partitionLeaders : clusterInfo.replicaLeaders(BrokerTopic.of(target, topic));
    candidate = candidate.isEmpty() ? partitionLeaders : candidate;
    return candidate.get((int) (Math.random() * candidate.size())).partition();
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
    var configuredFunctions = parseCostFunctionWeight(config);
    if (!configuredFunctions.isEmpty()) this.costFunction = HasBrokerCost.of(configuredFunctions);
    var customJmxPort = PartitionerUtils.parseIdJMXPort(config);
    var defaultJmxPort = config.integer(JMX_PORT);
    this.jmxPortGetter = id -> Optional.ofNullable(customJmxPort.get(id)).or(() -> defaultJmxPort);
    config
        .string(ROUND_ROBIN_LEASE_KEY)
        .map(Utils::toDuration)
        .ifPresent(d -> this.roundRobinLease = d);

    // put local mbean client first
    if (!metricCollector.listIdentities().contains(-1)) metricCollector.registerLocalJmx(-1);

    this.costFunction.fetcher().ifPresent(metricCollector::addFetcher);
    this.roundRobinKeeper = RoundRobinKeeper.of(ROUND_ROBIN_LENGTH, roundRobinLease);
  }

  /**
   * Helps parse cost-function names and weights. The format of the key and value is "<CostFunction
   * name>"="<weight>". For example, {"org.astraea.cost.broker.BrokerInputCost", "20"} will be
   * parsed to {(BrokerInputCost object), 20.0}.
   *
   * @param config that contains cost-function names and its corresponding weight
   * @return pairs of cost-function object and its corresponding weight
   */
  @SuppressWarnings("unchecked")
  public static Map<HasBrokerCost, Double> parseCostFunctionWeight(Configuration config) {
    return config.entrySet().stream()
        .map(
            nameAndWeight -> {
              Class<?> clz;
              try {
                clz = Class.forName(nameAndWeight.getKey());
              } catch (ClassNotFoundException ignore) {
                // this config is not cost function, so we just skip it.
                return null;
              }
              var weight = Double.parseDouble(nameAndWeight.getValue());
              if (weight < 0.0)
                throw new IllegalArgumentException("Cost-function weight should not be negative");
              return Map.entry(clz, weight);
            })
        .filter(Objects::nonNull)
        .filter(e -> HasBrokerCost.class.isAssignableFrom(e.getKey()))
        .collect(
            Collectors.toMap(
                e -> Utils.construct((Class<HasBrokerCost>) e.getKey(), config),
                Map.Entry::getValue));
  }

  @Override
  public void close() {
    metricCollector.close();
    super.close();
  }
}
