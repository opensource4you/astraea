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
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.ReplicaInfo;
import org.astraea.app.common.Utils;
import org.astraea.app.cost.CostFunction;
import org.astraea.app.cost.HasBrokerCost;
import org.astraea.app.cost.ReplicaLeaderCost;
import org.astraea.app.metrics.collector.BeanCollector;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.metrics.collector.Receiver;

/**
 * this dispatcher scores the nodes by multiples cost functions. Each function evaluate the target
 * node by different metrics. The default cost function ranks nodes by replica leader. It means the
 * node having lower replica leaders get higher score.
 *
 * <p>The requisite config is JMX port. Most cost functions need the JMX metrics to score nodes.
 * Normally, all brokers use the same JMX port, so you can just define the `jmx.port=12345`. If one
 * of brokers uses different JMX client port, you can define `broker.1000.jmx.port=11111` (`1000` is
 * the broker id) to replace the value of `jmx.port`.
 *
 * <p>You can configure the cost functions you want to use. By giving the name of that cost function
 * and its weight. For example,
 * `org.astraea.cost.ThroughputCost=1,org.astraea.cost.broker.BrokerOutputCost=1`.
 */
public class StrictCostDispatcher implements Dispatcher {
  public static final String JMX_PORT = "jmx.port";
  private static final Duration ROUND_ROBIN_LEASE = Duration.ofSeconds(30);

  private final BeanCollector beanCollector =
      BeanCollector.builder().interval(Duration.ofSeconds(4)).build();

  // The cost-functions we consider and the weight of them. It is visible for test
  Map<CostFunction, Double> functions = Map.of(new ReplicaLeaderCost.NoMetrics(), 1D);

  // all-in-one fetcher referenced to cost functions
  Optional<Fetcher> fetcher;

  Function<Integer, Integer> jmxPortGetter =
      (id) -> {
        throw new NoSuchElementException("broker: " + id + " does not have jmx port");
      };

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
                        .collect(
                            Collectors.toMap(
                                NodeInfo::id,
                                nodeInfo ->
                                    receiver(
                                        nodeInfo.host(),
                                        jmxPortGetter.apply(nodeInfo.id()),
                                        fetcher))))
            .orElse(Map.of()));

    // get latest beans for each node
    var beans =
        receivers.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().current()));

    if (roundRobin == null || System.currentTimeMillis() >= timeToUpdateRoundRobin) {
      roundRobin = newRoundRobin(functions, ClusterInfo.of(clusterInfo, beans));
      timeToUpdateRoundRobin = System.currentTimeMillis() + ROUND_ROBIN_LEASE.toMillis();
    }
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

  /**
   * create new Round-Robin based.
   *
   * @param costFunctions used to calculate weights
   * @param clusterInfo used to calculate costs
   * @return SmoothWeightedRoundRobin
   */
  static RoundRobin<Integer> newRoundRobin(
      Map<CostFunction, Double> costFunctions, ClusterInfo clusterInfo) {
    return RoundRobin.smoothWeighted(
        costFunctions.entrySet().stream()
            .filter(e -> e.getKey() instanceof HasBrokerCost)
            .map(e -> Map.entry((HasBrokerCost) e.getKey(), e.getValue()))
            .flatMap(
                functionWeight ->
                    functionWeight
                        .getKey()
                        // Execute all cost functions
                        .brokerCost(clusterInfo)
                        .value()
                        .entrySet()
                        .stream()
                        // Weight on cost functions result
                        .map(
                            IdScore ->
                                Map.entry(
                                    IdScore.getKey(),
                                    IdScore.getValue() * functionWeight.getValue())))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, Map.Entry::getValue, Double::sum, HashMap::new)));
  }

  @Override
  public void configure(Configuration config) {
    configure(
        parseCostFunctionWeight(config),
        config.integer(JMX_PORT),
        PartitionerUtils.parseIdJMXPort(config));
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
      Map<Integer, Integer> customJmxPort) {
    if (!functions.isEmpty()) this.functions = functions;
    this.fetcher = Fetcher.of(functions.keySet());
    this.jmxPortGetter =
        id ->
            Optional.ofNullable(customJmxPort.get(id))
                .or(() -> jmxPortDefault)
                .orElseThrow(
                    () -> new NoSuchElementException("broker: " + id + " does not have jmx port"));
    if (fetcher.isPresent() && jmxPortDefault.isEmpty() && customJmxPort.isEmpty())
      throw new IllegalArgumentException(
          "JMX port is empty but the cost functions need metrics from JMX server");
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
