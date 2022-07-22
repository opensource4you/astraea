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
package org.astraea.app.partitioner;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.ReplicaInfo;
import org.astraea.app.argument.DurationField;
import org.astraea.app.common.Utils;
import org.astraea.app.cost.CostFunction;
import org.astraea.app.cost.HasBrokerCost;
import org.astraea.app.cost.NodeLatencyCost;
import org.astraea.app.metrics.collector.BeanCollector;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.metrics.collector.Receiver;

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
  public static final String JMX_PORT = "jmx.port";
  public static final String ROUND_ROBIN_LEASE_KEY = "round.robin.lease";

  private final BeanCollector beanCollector =
      BeanCollector.builder().interval(Duration.ofSeconds(4)).build();

  Duration roundRobinLease;

  // The cost-functions we consider and the weight of them. It is visible for test
  Map<CostFunction, Double> functions = Map.of();

  // all-in-one fetcher referenced to cost functions
  Optional<Fetcher> fetcher;

  Function<Integer, Optional<Integer>> jmxPortGetter = (id) -> Optional.empty();

  final Map<Integer, Receiver> receivers = new TreeMap<>();

  volatile RoundRobin<Integer> roundRobin;
  volatile long timeToUpdateRoundRobin = -1;

  // visible for testing
  Receiver receiver(String host, int port, Fetcher fetcher) {
    return beanCollector.register().host(host).port(port).fetcher(fetcher).build();
  }

  @Override
  public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
    var partitionLeaders = clusterInfo.availableReplicaLeaders(topic);
    // just return first partition if there is no available partitions
    if (partitionLeaders.isEmpty()) return 0;

    // just return the only one available partition
    if (partitionLeaders.size() == 1) return partitionLeaders.iterator().next().partition();

    // add new receivers for new brokers
    receivers.putAll(
        fetcher
            .map(
                fetcher ->
                    partitionLeaders.stream()
                        .map(ReplicaInfo::nodeInfo)
                        .filter(nodeInfo -> !receivers.containsKey(nodeInfo.id()))
                        .distinct()
                        .filter(nodeInfo -> jmxPortGetter.apply(nodeInfo.id()).isPresent())
                        .collect(
                            Collectors.toMap(
                                NodeInfo::id,
                                nodeInfo ->
                                    receiver(
                                        nodeInfo.host(),
                                        jmxPortGetter.apply(nodeInfo.id()).get(),
                                        fetcher))))
            .orElse(Map.of()));

    tryToUpdateRoundRobin(clusterInfo);

    return roundRobin
        .next(partitionLeaders.stream().map(r -> r.nodeInfo().id()).collect(Collectors.toSet()))
        .flatMap(
            brokerId ->
                // TODO: which partition is better when all of them are in same node?
                partitionLeaders.stream()
                    .filter(r -> r.nodeInfo().id() == brokerId)
                    .map(ReplicaInfo::partition)
                    .findAny())
        .orElse(0);
  }

  void tryToUpdateRoundRobin(ClusterInfo clusterInfo) {
    if (roundRobin == null || System.currentTimeMillis() >= timeToUpdateRoundRobin) {
      roundRobin =
          newRoundRobin(
              functions,
              clusterInfo,
              ClusterBean.of(
                  receivers.entrySet().stream()
                      .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().current()))));
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
  static Map<Integer, Double> costToScore(Map<Integer, Double> cost) {
    var max = cost.values().stream().max(Double::compare);
    return max.map(
            m ->
                cost.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> m - e.getValue())))
        .orElse(cost);
  }

  /**
   * create new Round-Robin based.
   *
   * @param costFunctions used to calculate weights
   * @param clusterInfo used to calculate costs
   * @return SmoothWeightedRoundRobin
   */
  static RoundRobin<Integer> newRoundRobin(
      Map<CostFunction, Double> costFunctions, ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var weightedCost =
        costFunctions.entrySet().stream()
            .filter(e -> e.getKey() instanceof HasBrokerCost)
            .flatMap(
                functionWeight ->
                    ((HasBrokerCost) functionWeight.getKey())
                        .brokerCost(clusterInfo, clusterBean).value().entrySet().stream()
                            .map(
                                idAndCost ->
                                    Map.entry(
                                        idAndCost.getKey(),
                                        idAndCost.getValue() * functionWeight.getValue())))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, Map.Entry::getValue, Double::sum, HashMap::new));
    return RoundRobin.smooth(costToScore(weightedCost));
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
      Map<CostFunction, Double> functions,
      Optional<Integer> jmxPortDefault,
      Map<Integer, Integer> customJmxPort,
      Duration roundRobinLease) {
    this.functions = functions;
    // the temporary exception won't affect the smooth-weighted too much.
    // TODO: should we propagate the exception by better way? For example: Slf4j ?
    // see https://github.com/skiptests/astraea/issues/486
    this.fetcher =
        Fetcher.of(
            this.functions.keySet().stream()
                .map(CostFunction::fetcher)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toUnmodifiableList()),
            Throwable::printStackTrace);
    this.jmxPortGetter = id -> Optional.ofNullable(customJmxPort.get(id)).or(() -> jmxPortDefault);

    // put local mbean client first
    this.fetcher.ifPresent(
        f -> receivers.put(-1, beanCollector.register().local().fetcher(f).build()));
    this.roundRobinLease = roundRobinLease;
  }

  /**
   * Helps parse cost-function names and weights. The format of the key and value is "<CostFunction
   * name>"="<weight>". For example, {"org.astraea.cost.broker.BrokerInputCost", "20"} will be
   * parsed to {(BrokerInputCost object), 20.0}.
   *
   * @param config that contains cost-function names and its corresponding weight
   * @return pairs of cost-function object and its corresponding weight
   */
  public static Map<CostFunction, Double> parseCostFunctionWeight(Configuration config) {
    return config.entrySet().stream()
        .map(
            nameAndWeight -> {
              Class<?> name;
              double weight;
              try {
                name = Class.forName(nameAndWeight.getKey());
                weight = Double.parseDouble(nameAndWeight.getValue());
                if (weight < 0.0)
                  throw new IllegalArgumentException("Cost-function weight should not be negative");
              } catch (ClassNotFoundException ignore) {
                /* To delete all config option that is not for configuring cost-function. */
                return null;
              }
              return Map.entry(name, weight);
            })
        .filter(Objects::nonNull)
        .filter(e -> CostFunction.class.isAssignableFrom(e.getKey()))
        .map(
            e -> {
              try {
                return Map.entry(
                    (CostFunction) e.getKey().getConstructor().newInstance(), e.getValue());
              } catch (Exception ex) {
                ex.printStackTrace();
                throw new IllegalArgumentException(ex);
              }
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public void close() {
    receivers.values().forEach(r -> Utils.swallowException(r::close));
    receivers.clear();
  }
}
