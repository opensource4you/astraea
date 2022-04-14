package org.astraea.partitioner.smoothPartitioner;

import static org.astraea.partitioner.nodeLoadMetric.PartitionerUtils.partitionerConfig;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.astraea.Utils;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.CostFunction;
import org.astraea.cost.DynamicWeightsLoadCost;
import org.astraea.cost.PartitionInfo;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.BeanCollector;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.collector.Receiver;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.partitioner.Configuration;
import org.astraea.partitioner.Dispatcher;

/**
 * Based on the jmx metrics obtained from Kafka, it records the load status of the node over a
 * period of time. Predict the future status of each node through the poisson of the load status.
 * Finally, the result of poisson is used as the weight to perform dynamic weighted RoundRobin.
 */
public class DynamicWeightsDispatcher implements Dispatcher {
  public static final String JMX_PORT = "jmx.port";
  private final BeanCollector beanCollector =
      BeanCollector.builder()
          .interval(Duration.ofSeconds(1))
          .numberOfObjectsPerNode(1)
          .clientCreator(MBeanClient::jndi)
          .build();
  private Optional<Integer> jmxPortDefault = Optional.empty();
  private final Map<Integer, Integer> jmxPorts = new TreeMap<>();
  private final Map<Integer, Receiver> receivers = new TreeMap<>();

  private final Collection<CostFunction> functions = List.of(new DynamicWeightsLoadCost());

  private Map<Integer, Collection<HasBeanObject>> beans;
  private final ThreadLocalRandom random = ThreadLocalRandom.current();
  private List<PartitionInfo> partitions;

  @Override
  public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {

    partitions = clusterInfo.availablePartitions(topic);

    partitions.stream()
        .filter(p -> !receivers.containsKey(p.leader().id()))
        .forEach(
            p ->
                receivers.put(
                    p.leader().id(), receiver(p.leader().host(), jmxPort(p.leader().id()))));

    beans =
        receivers.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().current()));

    // just return first partition if there is no available partitions
    if (partitions.isEmpty()) return 0;

    // just return the only one available partition
    if (partitions.size() == 1) return partitions.iterator().next().partition();

    // TODO Will be removed when merged into StrictCostDispatcher
    var targetBroker =
        functions.stream().findFirst().get().cost(ClusterInfo.of(clusterInfo, beans));

    var sum = targetBroker.values().stream().mapToDouble(i -> i).sum();
    var target = random.nextDouble();
    var targetID = 0;
    for (var score : targetBroker.entrySet()) {
      target -= score.getValue() / sum;
      if (target < 0) {
        targetID = score.getKey();
        break;
      }
    }

    var finalTargetID = targetID;
    var targetPartitions =
        partitions.stream()
            .filter(p -> p.leader().id() == finalTargetID)
            .collect(Collectors.toList());

    return targetPartitions.get(random.nextInt(targetPartitions.size())).partition();
  }

  Receiver receiver(String host, int port) {
    return beanCollector
        .register()
        .host(host)
        .port(port)
        .fetcher(
            Fetcher.of(
                functions.stream()
                    .map(CostFunction::fetcher)
                    .collect(Collectors.toUnmodifiableList())))
        .build();
  }

  @Override
  public void configure(Configuration configs) {
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

  // visible for testing
  int jmxPort(int id) {
    if (jmxPorts.containsKey(id)) return jmxPorts.get(id);
    return jmxPortDefault.orElseThrow(
        () -> new NoSuchElementException("broker: " + id + " does not have jmx port"));
  }

  @Override
  public void close() {
    receivers.values().forEach(Utils::close);
    receivers.clear();
  }
}
