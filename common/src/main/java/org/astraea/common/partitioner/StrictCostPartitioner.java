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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.BrokerTopic;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.cost.BrokerCost;
import org.astraea.common.cost.HasBrokerCost;
import org.astraea.common.cost.NoSufficientMetricsException;
import org.astraea.common.cost.NodeLatencyCost;
import org.astraea.common.metrics.JndiClient;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.collector.MetricStore;
import org.astraea.common.producer.ProducerConfigs;

/**
 * this partitioner scores the nodes by multiples cost functions. Each function evaluate the target
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
public class StrictCostPartitioner extends Partitioner {
  public static final String METRIC_STORE_KEY = "metric.store";
  public static final String METRIC_STORE_TOPIC = "topic";
  public static final String METRIC_STORE_LOCAL = "local";
  static final int ROUND_ROBIN_LENGTH = 400;
  static final String JMX_PORT = "jmx.port";
  static final String ROUND_ROBIN_LEASE_KEY = "round.robin.lease";
  // visible for testing
  MetricStore metricStore = null;

  private Duration roundRobinLease = Duration.ofSeconds(4);
  HasBrokerCost costFunction = new NodeLatencyCost();
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
          () -> costToScore(costFunction.brokerCost(clusterInfo, metricStore.clusterBean())));
    } catch (NoSufficientMetricsException e) {
      // There is not enough metrics for the cost functions computing teh broker cost. We should not
      // update the round-robin keeper. Reuse the weights that were kept in the round-robin keeper.

      // Let the user know the cost-functions were complaining.
      e.printStackTrace();
    }

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
    var configuredFunctions =
        Utils.costFunctions(
            config.filteredPrefixConfigs(COST_PREFIX).raw(), HasBrokerCost.class, config);
    if (!configuredFunctions.isEmpty()) this.costFunction = HasBrokerCost.of(configuredFunctions);
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

    List<MetricStore.Receiver> receivers =
        switch (config.string(METRIC_STORE_KEY).orElse(METRIC_STORE_LOCAL)) {
          case METRIC_STORE_TOPIC -> List.of(
              MetricStore.Receiver.topic(
                  config.requireString(ProducerConfigs.BOOTSTRAP_SERVERS_CONFIG)),
              MetricStore.Receiver.local(
                  () -> CompletableFuture.completedStage(Map.of(-1, JndiClient.local()))));
          case METRIC_STORE_LOCAL -> {
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
                                          b.id(),
                                          JndiClient.of(b.host(), jmxPortGetter.apply(b.id()))));
                              // add local client to fetch consumer metrics
                              map.put(-1, JndiClient.local());
                              return Collections.unmodifiableMap(map);
                            });
            yield List.of(MetricStore.Receiver.local(clientSupplier));
          }
          default -> throw new IllegalArgumentException(
              "unknown metric store type: "
                  + config.string(METRIC_STORE_KEY)
                  + ". Use "
                  + METRIC_STORE_TOPIC
                  + " or "
                  + METRIC_STORE_LOCAL);
        };
    metricStore =
        MetricStore.builder()
            .receivers(receivers)
            .sensorsSupplier(() -> Map.of(this.costFunction.metricSensor(), (integer, e) -> {}))
            .build();

    this.roundRobinKeeper = RoundRobinKeeper.of(ROUND_ROBIN_LENGTH, roundRobinLease);
  }

  @Override
  public void close() {
    metricStore.close();
    super.close();
  }
}
