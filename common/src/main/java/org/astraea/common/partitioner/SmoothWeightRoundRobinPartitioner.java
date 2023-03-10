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
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.BrokerTopic;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.cost.NeutralIntegratedCost;
import org.astraea.common.metrics.collector.LocalMetricCollector;
import org.astraea.common.metrics.collector.MetricCollector;

public class SmoothWeightRoundRobinPartitioner extends Partitioner {
  private static final int ROUND_ROBIN_LENGTH = 400;
  private static final String JMX_PORT = "jmx.port";
  public static final String ROUND_ROBIN_LEASE_KEY = "round.robin.lease";
  private final ConcurrentLinkedDeque<Integer> unusedPartitions = new ConcurrentLinkedDeque<>();

  private final NeutralIntegratedCost neutralIntegratedCost = new NeutralIntegratedCost();
  private final MetricCollector metricCollector =
      MetricCollector.local()
          .interval(Duration.ofMillis(1500))
          .addMetricSensor(neutralIntegratedCost.metricSensor().get())
          .build();

  private SmoothWeightCal<Integer> smoothWeightCal;
  private RoundRobinKeeper roundRobinKeeper;
  private Function<Integer, Optional<Integer>> jmxPortGetter = (id) -> Optional.empty();

  @Override
  public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
    var partitionLeaders = clusterInfo.replicaLeaders(topic);
    // just return first partition if there is no available partitions
    if (partitionLeaders.isEmpty()) return 0;

    // just return the only one available partition
    if (partitionLeaders.size() == 1) return partitionLeaders.get(0).partition();

    var targetPartition = unusedPartitions.poll();
    refreshPartitionMetaData(clusterInfo, topic);
    Supplier<Map<Integer, Double>> supplier =
        () ->
            // fetch the latest beans for each node
            neutralIntegratedCost.brokerCost(clusterInfo, metricCollector.clusterBean()).value();

    smoothWeightCal.refresh(supplier);

    if (targetPartition == null) {
      roundRobinKeeper.tryToUpdate(clusterInfo, smoothWeightCal.effectiveWeightResult);
      var target = roundRobinKeeper.next();

      var candidate =
          target < 0 ? partitionLeaders : clusterInfo.replicaLeaders(BrokerTopic.of(target, topic));
      candidate = candidate.isEmpty() ? partitionLeaders : candidate;

      targetPartition = candidate.get((int) (Math.random() * candidate.size())).partition();
    }

    return targetPartition;
  }

  @Override
  public void close() {
    metricCollector.close();
  }

  @Override
  public void configure(Configuration configuration) {
    configure(
        configuration.integer(JMX_PORT),
        PartitionerUtils.parseIdJMXPort(configuration),
        configuration
            .string(ROUND_ROBIN_LEASE_KEY)
            .map(Utils::toDuration)
            // The duration of updating beans is 4 seconds, so
            // the default duration of updating RR is 4 seconds.
            .orElse(Duration.ofSeconds(4)));
  }

  void configure(
      Optional<Integer> jmxPortDefault,
      Map<Integer, Integer> customJmxPort,
      Duration roundRobinLease) {
    this.jmxPortGetter = id -> Optional.ofNullable(customJmxPort.get(id)).or(() -> jmxPortDefault);
    this.roundRobinKeeper = RoundRobinKeeper.of(ROUND_ROBIN_LENGTH, roundRobinLease);
    this.smoothWeightCal =
        new SmoothWeightCal<>(
            customJmxPort.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, ignore -> 1.0)));
  }

  @Override
  public void onNewBatch(String topic, int prevPartition, ClusterInfo clusterInfo) {
    unusedPartitions.add(prevPartition);
  }

  private void refreshPartitionMetaData(ClusterInfo clusterInfo, String topic) {
    if (this.metricCollector instanceof LocalMetricCollector) {
      var localCollector = (LocalMetricCollector) this.metricCollector;
      clusterInfo.availableReplicas(topic).stream()
          .filter(p -> !localCollector.listIdentities().contains(p.nodeInfo().id()))
          .forEach(
              node -> {
                if (!localCollector.listIdentities().contains(node.nodeInfo().id())) {
                  jmxPortGetter
                      .apply(node.nodeInfo().id())
                      .ifPresent(
                          port ->
                              localCollector.registerJmx(
                                  node.nodeInfo().id(),
                                  InetSocketAddress.createUnresolved(
                                      node.nodeInfo().host(), port)));
                }
              });
    }
  }
}
