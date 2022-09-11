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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.cost.Configuration;
import org.astraea.common.cost.NeutralIntegratedCost;
import org.astraea.common.cost.Periodic;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.collector.BeanCollector;
import org.astraea.common.metrics.collector.Receiver;
import org.astraea.common.partitioner.Dispatcher;
import org.astraea.common.partitioner.PartitionerUtils;

public class SmoothWeightRoundRobinDispatcher extends Periodic<Map<Integer, Double>>
    implements Dispatcher {
  private final ConcurrentLinkedDeque<Integer> unusedPartitions = new ConcurrentLinkedDeque<>();
  private final ConcurrentMap<String, BrokerNextCounter> topicCounter = new ConcurrentHashMap<>();
  private final BeanCollector beanCollector =
      BeanCollector.builder()
          .interval(Duration.ofSeconds(1))
          .numberOfObjectsPerNode(1)
          .clientCreator(MBeanClient::jndi)
          .build();
  private final Optional<Integer> jmxPortDefault = Optional.empty();
  private final Map<Integer, Integer> jmxPorts = new TreeMap<>();
  private final Map<Integer, Receiver> receivers = new TreeMap<>();

  private final Map<Integer, List<Integer>> hasPartitions = new TreeMap<>();

  private SmoothWeightRoundRobin smoothWeightRoundRobinCal;

  private final NeutralIntegratedCost neutralIntegratedCost = new NeutralIntegratedCost();

  private Map<Integer, Collection<HasBeanObject>> beans;
  private List<ReplicaInfo> partitions;

  public static final String JMX_PORT = "jmx.port";

  @Override
  public int partition(
      String topic, byte[] key, byte[] value, ClusterInfo<ReplicaInfo> clusterInfo) {
    var targetPartition = unusedPartitions.poll();
    tryUpdateAfterOneSecond(
        () -> {
          refreshPartitionMetaData(clusterInfo, topic);
          // fetch the latest beans for each node
          beans =
              receivers.entrySet().stream()
                  .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().current()));

          var compoundScore =
              neutralIntegratedCost.brokerCost(clusterInfo, ClusterBean.of(beans)).value();

          if (smoothWeightRoundRobinCal == null) {
            smoothWeightRoundRobinCal = new SmoothWeightRoundRobin(compoundScore);
          }
          smoothWeightRoundRobinCal.init(compoundScore);

          return compoundScore;
        });
    // just return first partition if there is no available partitions
    if (partitions.isEmpty()) return 0;

    // just return the only one available partition
    if (partitions.size() == 1) return partitions.iterator().next().partition();

    if (targetPartition == null) {
      var targetBroker = smoothWeightRoundRobinCal.getAndChoose(topic, clusterInfo);
      targetPartition =
          hasPartitions
              .get(targetBroker)
              .get(
                  nextValue(topic, clusterInfo, targetBroker)
                      % hasPartitions.get(targetBroker).size());
    }

    return targetPartition;
  }

  @Override
  public void closeDispatcher() {
    receivers.values().forEach(r -> Utils.swallowException(r::close));
    receivers.clear();
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

  Receiver receiver(String host, int port) {
    return beanCollector
        .register()
        .host(host)
        .port(port)
        // TODO: handle the empty fetcher
        .fetcher(neutralIntegratedCost.fetcher().get())
        .build();
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
    partitions.forEach(
        p ->
            hasPartitions
                .computeIfAbsent(p.nodeInfo().id(), k -> new ArrayList<>())
                .add(p.partition()));

    partitions.stream()
        .filter(p -> !receivers.containsKey(p.nodeInfo().id()))
        .forEach(
            p ->
                receivers.put(
                    p.nodeInfo().id(), receiver(p.nodeInfo().host(), jmxPort(p.nodeInfo().id()))));
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
