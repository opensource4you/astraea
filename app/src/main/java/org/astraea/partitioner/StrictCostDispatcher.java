package org.astraea.partitioner;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.astraea.Utils;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.CostFunction;
import org.astraea.cost.HasBrokerCost;
import org.astraea.cost.ReplicaInfo;
import org.astraea.metrics.collector.BeanCollector;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.collector.Receiver;

/**
 * this dispatcher scores the nodes by multiples cost functions. Each function evaluate the target
 * node by different metrics. The default cost function ranks nodes by throughput. It means the node
 * having lower throughput get higher score.
 *
 * <p>The requisite config is JMX port. Most cost functions need the JMX metrics to score nodes.
 * Normally, all brokers use the same JMX port, so you can just define the `jmx.port=12345`. If one
 * of brokers uses different JMX client port, you can define `broker.1000.jmx.port=11111` (`1000` is
 * the broker id) to replace the value of `jmx.port`.
 */
public class StrictCostDispatcher implements Dispatcher {
  public static final String JMX_PORT = "jmx.port";

  private final BeanCollector beanCollector =
      BeanCollector.builder().interval(Duration.ofSeconds(4)).build();
  private final Collection<CostFunction> functions;
  private Optional<Integer> jmxPortDefault = Optional.empty();
  private final Map<Integer, Integer> jmxPorts = new TreeMap<>();
  final Map<Integer, Receiver> receivers = new TreeMap<>();

  public StrictCostDispatcher() {
    this(List.of(CostFunction.throughput()));
  }

  // visible for testing
  StrictCostDispatcher(Collection<CostFunction> functions) {
    this.functions = functions;
  }

  @Override
  public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
    var partitions = clusterInfo.availablePartitions(topic);
    // just return first partition if there is no available partitions
    if (partitions.isEmpty()) return 0;

    // just return the only one available partition
    if (partitions.size() == 1) return partitions.iterator().next().partition();

    // add new receivers for new brokers
    partitions.stream()
        .filter(p -> !receivers.containsKey(p.leader().id()))
        .forEach(
            p ->
                receivers.put(
                    p.leader().id(), receiver(p.leader().host(), jmxPort(p.leader().id()))));

    // get latest beans for each node
    var beans =
        receivers.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().current()));

    // get scores from all cost functions
    var scores =
        functions.stream()
            .filter(f -> f instanceof HasBrokerCost)
            .map(f -> (HasBrokerCost) f)
            .map(f -> f.brokerCost(ClusterInfo.of(clusterInfo, beans)).value())
            .collect(Collectors.toUnmodifiableList());

    return bestPartition(partitions, scores).map(e -> e.getKey().partition()).orElse(0);
  }

  // visible for testing
  static Optional<Map.Entry<ReplicaInfo, Double>> bestPartition(
      List<ReplicaInfo> partitions, List<Map<Integer, Double>> scores) {
    return partitions.stream()
        .map(
            p ->
                Map.entry(
                    p,
                    scores.stream().mapToDouble(s -> s.getOrDefault(p.leader().id(), 0.0D)).sum()))
        .min(Map.Entry.comparingByValue());
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
  public void configure(Configuration config) {
    jmxPortDefault = config.integer(JMX_PORT);

    // seeks for custom jmx ports.
    config.entrySet().stream()
        .filter(e -> e.getKey().startsWith("broker."))
        .filter(e -> e.getKey().endsWith(JMX_PORT))
        .forEach(e -> jmxPorts.put(Integer.parseInt(e.getKey()), Integer.parseInt(e.getValue())));
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
