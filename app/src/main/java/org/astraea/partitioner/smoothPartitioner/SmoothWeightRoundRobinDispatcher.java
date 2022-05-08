package org.astraea.partitioner.smoothPartitioner;

import static org.astraea.partitioner.nodeLoadMetric.PartitionerUtils.partitionerConfig;

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
import org.astraea.Utils;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.NodeInfo;
import org.astraea.cost.Periodic;
import org.astraea.cost.ReplicaInfo;
import org.astraea.cost.brokersMetrics.NeutralIntegratedCost;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.BeanCollector;
import org.astraea.metrics.collector.Receiver;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.partitioner.Configuration;
import org.astraea.partitioner.Dispatcher;

public class SmoothWeightRoundRobinDispatcher extends Periodic<Void> implements Dispatcher {
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

  private final SmoothWeightRoundRobin smoothWeightRoundRobinCal = new SmoothWeightRoundRobin();

  private final NeutralIntegratedCost neutralIntegratedCost = new NeutralIntegratedCost();

  private Map<Integer, Collection<HasBeanObject>> beans;
  private List<ReplicaInfo> partitions;

  public static final String JMX_PORT = "jmx.port";

  @Override
  public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
    var targetPartition = unusedPartitions.poll();
    tryUpdate(
        () -> {
          refreshPartitionMetaData(clusterInfo, topic);
          // fetch the latest beans for each node
          beans =
              receivers.entrySet().stream()
                  .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().current()));

          var compoundScore =
              neutralIntegratedCost.brokerCost(ClusterInfo.of(clusterInfo, beans)).value();

          smoothWeightRoundRobinCal.init(compoundScore);
          return null;
        },
        1);
    // just return first partition if there is no available partitions
    if (partitions.isEmpty()) return 0;

    // just return the only one available partition
    if (partitions.size() == 1) return partitions.iterator().next().partition();

    if (targetPartition == null) {
      var targetBroker = smoothWeightRoundRobinCal.getAndChoose();
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
  public void configure(Configuration config) {
    Dispatcher.super.configure(config);
  }

  @Override
  public void close() {
    receivers.values().forEach(Utils::close);
    receivers.clear();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    var properties = partitionerConfig(configs);
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
        .fetcher(neutralIntegratedCost.fetcher())
        .build();
  }

  private int nextValue(String topic, ClusterInfo clusterInfo, int targetBroker) {
    return topicCounter
        .computeIfAbsent(topic, k -> new BrokerNextCounter(clusterInfo))
        .brokerCounter
        .get(targetBroker)
        .getAndIncrement();
  }

  private void refreshPartitionMetaData(ClusterInfo clusterInfo, String topic) {
    partitions = clusterInfo.availablePartitions(topic);
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

    BrokerNextCounter(ClusterInfo clusterInfo) {
      brokerCounter =
          clusterInfo.nodes().stream()
              .collect(Collectors.toMap(NodeInfo::id, node -> new AtomicInteger(0)));
    }
  }
}
