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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.astraea.app.common.Utils;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.cost.CostFunction;
import org.astraea.app.cost.HasBrokerCost;
import org.astraea.app.cost.ReplicaInfo;
import org.astraea.app.metrics.collector.BeanCollector;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.metrics.collector.Receiver;

/**
 * this dispatcher scores the nodes by multiples cost functions. Each function evaluate the target
 * node by different metrics. The default cost function ranks nodes by throughput. It means the node
 * having lower throughput get higher score.
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

  private final BeanCollector beanCollector =
      BeanCollector.builder().interval(Duration.ofSeconds(4)).build();

  /* The cost-functions we consider and the weight of them. It is visible for test.*/
  Map<CostFunction, Double> functions;
  private Optional<Integer> jmxPortDefault = Optional.empty();
  private final Map<Integer, Integer> jmxPorts = new TreeMap<>();
  final Map<Integer, Receiver> receivers = new TreeMap<>();

  public StrictCostDispatcher() {
    this(List.of(CostFunction.throughput()));
  }

  // visible for testing
  StrictCostDispatcher(Collection<CostFunction> functions) {
    this.functions =
        functions.stream()
            .map(f -> Map.entry(f, 1.0))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
    var partitionLeaders = clusterInfo.availableReplicaLeaders(topic);
    // just return first partition if there is no available partitions
    if (partitionLeaders.isEmpty()) return 0;

    // just return the only one available partition
    if (partitionLeaders.size() == 1) return partitionLeaders.iterator().next().partition();

    // add new receivers for new brokers
    partitionLeaders.stream()
        .filter(p -> !receivers.containsKey(p.nodeInfo().id()))
        .forEach(
            p ->
                receivers.put(
                    p.nodeInfo().id(), receiver(p.nodeInfo().host(), jmxPort(p.nodeInfo().id()))));

    // get latest beans for each node
    var beans =
        receivers.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().current()));

    // get scores from all cost functions
    var scores = computeScore(functions, ClusterInfo.of(clusterInfo, beans));

    return bestPartition(partitionLeaders, scores).map(e -> e.getKey().partition()).orElse(0);
  }

  /**
   * Pass clusterInfo into all cost-functions. The result of each cost-function will multiply on
   * their corresponding weight.
   *
   * @param functions the cost-function objects and their corresponding weight
   * @return cost-function result multiplied on their corresponding weight
   */
  static List<Map<Integer, Double>> computeScore(
      Map<CostFunction, Double> functions, ClusterInfo clusterInfo) {
    return functions.entrySet().stream()
        .filter(e -> e.getKey() instanceof HasBrokerCost)
        .map(e -> Map.entry((HasBrokerCost) e.getKey(), e.getValue()))
        .map(
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
                                IdScore.getKey(), IdScore.getValue() * functionWeight.getValue()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
        .collect(Collectors.toUnmodifiableList());
  }

  // visible for testing
  static Optional<Map.Entry<ReplicaInfo, Double>> bestPartition(
      List<ReplicaInfo> partitions, List<Map<Integer, Double>> scores) {
    return partitions.stream()
        .map(
            p ->
                Map.entry(
                    p,
                    scores.stream()
                        .mapToDouble(s -> s.getOrDefault(p.nodeInfo().id(), 0.0D))
                        .sum()))
        .min(Map.Entry.comparingByValue());
  }

  Receiver receiver(String host, int port) {
    return beanCollector
        .register()
        .host(host)
        .port(port)
        // TODO: return optional receiver
        .fetcher(Fetcher.of(functions.keySet()).get())
        .build();
  }

  @Override
  public void configure(Configuration config) {
    jmxPortDefault = config.integer(JMX_PORT);

    // seeks for custom jmx ports.
    jmxPorts.putAll(PartitionerUtils.parseIdJMXPort(config));

    functions = parseCostFunctionWeight(config);
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

  // visible for testing
  int jmxPort(int id) {
    if (jmxPorts.containsKey(id)) return jmxPorts.get(id);
    return jmxPortDefault.orElseThrow(
        () -> new NoSuchElementException("broker: " + id + " does not have jmx port"));
  }

  @Override
  public void close() {
    receivers.values().forEach(r -> Utils.swallowException(r::close));
    receivers.clear();
  }
}
