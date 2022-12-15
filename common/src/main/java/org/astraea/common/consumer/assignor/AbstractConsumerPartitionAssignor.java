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
package org.astraea.common.consumer.assignor;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.cost.HasPartitionCost;
import org.astraea.common.cost.ReplicaSizeCost;
import org.astraea.common.metrics.collector.MetricCollector;
import org.astraea.common.partitioner.PartitionerUtils;

/** Abstract assignor implementation which does some common work (e.g., configuration). */
public abstract class AbstractConsumerPartitionAssignor implements ConsumerPartitionAssignor {
  public static final String JMX_PORT = "jmx.port";
  Function<Integer, Optional<Integer>> jmxPortGetter = (id) -> Optional.empty();
  HasPartitionCost costFunction = HasPartitionCost.EMPTY;
  // TODO: metric collector may be configured by user in the future.
  // TODO: need to track the performance when using the assignor in large scale consumers, see
  // https://github.com/skiptests/astraea/pull/1162#discussion_r1036285677
  private final MetricCollector metricCollector =
      MetricCollector.builder()
          .interval(Duration.ofSeconds(1))
          .expiration(Duration.ofSeconds(15))
          .build();

  /**
   * Perform the group assignment given the members' subscription and ClusterInfo.
   *
   * @param subscriptions Map from the member id to their respective topic subscription.
   * @param metadata Current topic/broker metadata known by consumer.
   * @return Map from each member to the list of topic-partitions assigned to them.
   */
  public abstract Map<String, List<TopicPartition>> assign(
      Map<String, org.astraea.common.consumer.assignor.Subscription> subscriptions,
      ClusterInfo<ReplicaInfo> metadata);

  /**
   * Parse config to get JMX port and cost function type.
   *
   * @param config configuration
   */
  @Override
  public void configure(Configuration config) {
    var costFunctions = parseCostFunctionWeight(config);
    var customJMXPort = PartitionerUtils.parseIdJMXPort(config);
    var defaultJMXPort = config.integer(JMX_PORT);

    this.costFunction =
        costFunctions.isEmpty()
            ? HasPartitionCost.of(Map.of(new ReplicaSizeCost(), 1D))
            : HasPartitionCost.of(costFunctions);
    this.jmxPortGetter = id -> Optional.ofNullable(customJMXPort.get(id)).or(() -> defaultJMXPort);
    this.costFunction.fetcher().ifPresent(metricCollector::addFetcher);
  }

  /**
   * check the nodes which wasn't register yet.
   *
   * @param nodes List of node information
   * @return Map from each broker id to broker host
   */
  @Override
  public Map<Integer, String> checkUnregister(List<NodeInfo> nodes) {
    return nodes.stream()
        .filter(i -> !metricCollector.listIdentities().contains(i.id()))
        .collect(Collectors.toMap(NodeInfo::id, NodeInfo::host));
  }

  /**
   * register the JMX for metric collector. only register the JMX that is not registered yet.
   *
   * @param unregister Map from each broker id to broker host
   */
  @Override
  public void registerJMX(Map<Integer, String> unregister) {
    unregister.forEach(
        (id, host) ->
            metricCollector.registerJmx(
                id, InetSocketAddress.createUnresolved(host, jmxPortGetter.apply(id).get())));
  }

  // used for test
  protected void registerLocalJMX(Map<Integer, String> unregister) {
    unregister.forEach((id, host) -> metricCollector.registerLocalJmx(id));
  }

  /**
   * Parse cost function names and weight. you can specify multiple cost function with assignor. The
   * format of key and value pair is "<CostFunction name>"="<weight>". For instance,
   * {"org.astraea.common.cost.ReplicaSizeCost","1"} will be parsed to {(HasPartitionCost object),
   * 1.0}.
   *
   * @param config the configuration of the user setting, contain cost function and its weight.
   * @return Map from cost function object to its weight
   */
  static Map<HasPartitionCost, Double> parseCostFunctionWeight(Configuration config) {
    return config.entrySet().stream()
        .map(
            nameAndWeight -> {
              Class<?> clz;
              try {
                clz = Class.forName(nameAndWeight.getKey());
              } catch (ClassNotFoundException ignore) {
                return null;
              }
              var weight = Double.parseDouble(nameAndWeight.getValue());
              if (weight < 0.0)
                throw new IllegalArgumentException("Cost function weight should not be negative");
              return Map.entry(clz, weight);
            })
        .filter(Objects::nonNull)
        .filter(e -> HasPartitionCost.class.isAssignableFrom(e.getKey()))
        .collect(
            Collectors.toMap(
                e -> Utils.construct((Class<HasPartitionCost>) e.getKey(), config),
                Map.Entry::getValue));
  }
}
