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
package org.astraea.common.partitioner.smooth;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.astraea.common.Configuration;
import org.astraea.common.Lazy;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.cost.NeutralIntegratedCost;
import org.astraea.common.metrics.collector.MetricCollector;
import org.astraea.common.partitioner.Dispatcher;
import org.astraea.common.partitioner.PartitionerUtils;

public class SmoothWeightRoundRobinDispatcher implements Dispatcher {
  private final ConcurrentLinkedDeque<Integer> unusedPartitions = new ConcurrentLinkedDeque<>();
  private final ConcurrentMap<String, BrokerNextCounter> topicCounter = new ConcurrentHashMap<>();
  private final MetricCollector metricCollector =
      MetricCollector.builder()
          .interval(Duration.ofSeconds(1))
          .expiration(Duration.ofSeconds(10))
          .build();
  private final Optional<Integer> jmxPortDefault = Optional.empty();
  private final Map<Integer, Integer> jmxPorts = new TreeMap<>();

  private final Lazy<SmoothWeightRoundRobin> smoothWeightRoundRobinCal = Lazy.of();

  private final NeutralIntegratedCost neutralIntegratedCost = new NeutralIntegratedCost();

  private List<ReplicaInfo> partitions;

  public static final String JMX_PORT = "jmx.port";

  @Override
  public int partition(
      String topic, byte[] key, byte[] value, ClusterInfo<ReplicaInfo> clusterInfo) {
    var targetPartition = unusedPartitions.poll();
    refreshPartitionMetaData(clusterInfo, topic);
    Supplier<Map<Integer, Double>> supplier =
        () -> {
          // fetch the latest beans for each node
          var beans = metricCollector.clusterBean().all();

          return neutralIntegratedCost.brokerCost(clusterInfo, ClusterBean.of(beans)).value();
        };
    // just return first partition if there is no available partitions
    if (partitions.isEmpty()) return 0;

    // just return the only one available partition
    if (partitions.size() == 1) return partitions.iterator().next().partition();

    if (targetPartition == null) {
      var smooth = smoothWeightRoundRobinCal.get(() -> new SmoothWeightRoundRobin(supplier.get()));
      smooth.init(supplier);
      var targetBroker = smooth.getAndChoose(topic, clusterInfo);
      var targetPartitions =
          clusterInfo.availableReplicas(topic).stream()
              .filter(r -> r.nodeInfo().id() == targetBroker)
              .collect(Collectors.toUnmodifiableList());
      targetPartition =
          targetPartitions
              .get(nextValue(topic, clusterInfo, targetBroker) % targetPartitions.size())
              .partition();
    }

    return targetPartition;
  }

  @Override
  public void doClose() {
    metricCollector.close();
  }

  @Override
  public void configure(Configuration configuration) {
    var properties =
        PartitionerUtils.partitionerConfig(
            configuration.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    var config =
        Configuration.of(
            properties.entrySet().stream()
                .collect(
                    Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString())));

    // seeks for custom jmx ports.
    config.entrySet().stream()
        .filter(e -> e.getKey().startsWith("broker."))
        .filter(e -> e.getKey().endsWith(JMX_PORT))
        .forEach(
            e ->
                jmxPorts.put(
                    Integer.parseInt(e.getKey().split("\\.")[1]), Integer.parseInt(e.getValue())));
  }

  @Override
  public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
    unusedPartitions.add(prevPartition);
  }

  int jmxPort(int id) {
    if (jmxPorts.containsKey(id)) return jmxPorts.get(id);
    return jmxPortDefault.orElseThrow(
        () -> new NoSuchElementException("broker: " + id + " does not have jmx port"));
  }

  private int nextValue(String topic, ClusterInfo<ReplicaInfo> clusterInfo, int targetBroker) {
    return topicCounter
        .computeIfAbsent(topic, k -> new BrokerNextCounter(clusterInfo))
        .brokerCounter
        .get(targetBroker)
        .getAndIncrement();
  }

  private void refreshPartitionMetaData(ClusterInfo<ReplicaInfo> clusterInfo, String topic) {
    partitions = clusterInfo.availableReplicas(topic);
    neutralIntegratedCost
        .fetcher()
        .ifPresent(
            fetcher -> {
              partitions.stream()
                  .filter(p -> !metricCollector.listIdentities().contains(p.nodeInfo().id()))
                  .forEach(
                      p -> {
                        metricCollector.registerJmx(
                            p.nodeInfo().id(),
                            InetSocketAddress.createUnresolved(
                                p.nodeInfo().host(), jmxPort(p.nodeInfo().id())));
                        metricCollector.addFetcher(fetcher);

                        // Wait until the initial value of metrics is exists.
                        while (metricCollector.listMetricTypes().stream()
                                .map(x -> metricCollector.metrics(x, p.nodeInfo().id(), 0))
                                .mapToInt(List::size)
                                .sum()
                            == 0) {
                          Utils.sleep(Duration.ofMillis(5));
                        }
                      });
            });
  }

  private static class BrokerNextCounter {
    private final Map<Integer, AtomicInteger> brokerCounter;

    BrokerNextCounter(ClusterInfo<ReplicaInfo> clusterInfo) {
      brokerCounter =
          clusterInfo.nodes().stream()
              .collect(Collectors.toMap(NodeInfo::id, node -> new AtomicInteger(0)));
    }
  }
}
