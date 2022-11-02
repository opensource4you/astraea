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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.argument.DurationField;
import org.astraea.common.cost.BrokerCost;
import org.astraea.common.cost.Configuration;
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
public class StrictCostDispatcher implements Dispatcher {
  static final int ROUND_ROBIN_LENGTH = 400;

  public static final String JMX_PORT = "jmx.port";
  public static final String ROUND_ROBIN_LEASE_KEY = "round.robin.lease";

  // visible for testing
  final MetricCollector metricCollector =
      MetricCollector.builder().interval(Duration.ofSeconds(4)).build();

  Duration roundRobinLease;

  HasBrokerCost costFunction = HasBrokerCost.EMPTY;
  Function<Integer, Optional<Integer>> jmxPortGetter = (id) -> Optional.empty();

  final int[] roundRobin = new int[ROUND_ROBIN_LENGTH];

  final AtomicInteger next = new AtomicInteger(0);

  volatile long timeToUpdateRoundRobin = -1;

  void tryToUpdateFetcher(ClusterInfo<ReplicaInfo> clusterInfo) {
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
                            metricCollector.addFetcher(fetcher);
                          }
                        }));
  }

  @Override
  public int partition(
      String topic, byte[] key, byte[] value, ClusterInfo<ReplicaInfo> clusterInfo) {
    var partitionLeaders = clusterInfo.replicaLeaders(topic);
    // just return first partition if there is no available partitions
    if (partitionLeaders.isEmpty()) return 0;

    // just return the only one available partition
    if (partitionLeaders.size() == 1) return partitionLeaders.get(0).partition();

    tryToUpdateFetcher(clusterInfo);

    tryToUpdateRoundRobin(clusterInfo);

    var target =
        roundRobin[
            next.getAndUpdate(previous -> previous >= roundRobin.length - 1 ? 0 : previous + 1)];

    // TODO: if the topic partitions are existent in fewer brokers, the target gets -1 in most cases
    var candidate = target < 0 ? partitionLeaders : clusterInfo.replicaLeaders(target, topic);
    candidate = candidate.isEmpty() ? partitionLeaders : candidate;
    return candidate.get((int) (Math.random() * candidate.size())).partition();
  }

  synchronized void tryToUpdateRoundRobin(ClusterInfo<ReplicaInfo> clusterInfo) {
    if (System.currentTimeMillis() >= timeToUpdateRoundRobin) {
      var roundRobin =
          RoundRobin.smooth(
              costToScore(costFunction.brokerCost(clusterInfo, metricCollector.clusterBean())));
      var ids =
          clusterInfo.nodes().stream().map(NodeInfo::id).collect(Collectors.toUnmodifiableSet());
      // TODO: make ROUND_ROBIN_LENGTH configurable ???
      IntStream.range(0, ROUND_ROBIN_LENGTH)
          .forEach(index -> this.roundRobin[index] = roundRobin.next(ids).orElse(-1));
      timeToUpdateRoundRobin = System.currentTimeMillis() + roundRobinLease.toMillis();
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
    var max = cost.value().values().stream().max(Double::compare);
    return max.map(
            m ->
                cost.value().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> m - e.getValue())))
        .orElse(cost.value());
  }

  @Override
  public void configure(Configuration config) {
    var configuredFunctions = parseCostFunctionWeight(config);
    configure(
        configuredFunctions.isEmpty() ? Map.of(new NodeLatencyCost(), 1D) : configuredFunctions,
        config.integer(JMX_PORT),
        PartitionerUtils.parseIdJMXPort(config),
        config
            .string(ROUND_ROBIN_LEASE_KEY)
            .map(DurationField::toDuration)
            // The duration of updating beans is 4 seconds, so
            // the default duration of updating RR is 4 seconds.
            .orElse(Duration.ofSeconds(4)));
  }

  /**
   * configure this StrictCostDispatcher. This method is extracted for testing.
   *
   * @param functions cost functions used by this dispatcher.
   * @param jmxPortDefault jmx port by default
   * @param customJmxPort jmx port for each node
   */
  void configure(
      Map<HasBrokerCost, Double> functions,
      Optional<Integer> jmxPortDefault,
      Map<Integer, Integer> customJmxPort,
      Duration roundRobinLease) {
    this.costFunction = HasBrokerCost.of(functions);
    this.jmxPortGetter = id -> Optional.ofNullable(customJmxPort.get(id)).or(() -> jmxPortDefault);

    // put local mbean client first
    if (!metricCollector.listIdentities().contains(-1)) {
      this.costFunction
          .fetcher()
          .ifPresent(
              fetcher -> {
                metricCollector.registerLocalJmx(-1);
                metricCollector.addFetcher(fetcher);
              });
      this.roundRobinLease = roundRobinLease;
    }
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
  public void doClose() {
    metricCollector.close();
  }
}
