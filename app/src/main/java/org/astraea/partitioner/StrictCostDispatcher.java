package org.astraea.partitioner;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.Utils;
import org.astraea.metrics.collector.BeanCollector;
import org.astraea.metrics.collector.Receiver;
import org.astraea.partitioner.cost.CostFunction;

public class StrictCostDispatcher implements Dispatcher {
  public static final String JMX_SERVERS_KEY = "jmx_servers";

  private final BeanCollector beanCollector =
      BeanCollector.builder().interval(Duration.ofSeconds(4)).build();
  private final Collection<CostFunction> functions = List.of(CostFunction.throughput());
  private Map<NodeId, Receiver> receivers;
  private Map<Integer, Integer> idAndJmxPort;

  @Override
  public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
    var partitions = clusterInfo.availablePartitions(topic);
    // just return first partition if there is no available partitions
    if (partitions.isEmpty()) return 0;

    if (receivers == null || receivers.isEmpty()) {
      var availableJmxNodes = jmxNodes(idAndJmxPort, clusterInfo);
      receivers =
          availableJmxNodes.stream()
              .collect(
                  Collectors.toMap(
                      n -> n,
                      n ->
                          beanCollector
                              .register()
                              .host(n.host())
                              .port(n.port())
                              .metricsGetters(
                                  functions.stream()
                                      .flatMap(c -> c.metricsGetters().stream())
                                      .collect(Collectors.toUnmodifiableList()))
                              .build()));
    }

    // can't find broker node, so we just return first partition.
    if (receivers.isEmpty()) return 0;

    // get latest beans for each node
    var beans =
        receivers.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().current()));

    // get scores from all cost functions
    var scores =
        functions.stream()
            .map(f -> f.cost(beans, clusterInfo))
            .collect(Collectors.toUnmodifiableList());

    // return the partition (node) having min score
    return partitions.stream()
        .map(
            p ->
                Map.entry(
                    p, scores.stream().mapToDouble(s -> s.getOrDefault(p.leader(), 0.0D)).sum()))
        .min(Map.Entry.comparingByValue())
        .map(e -> e.getKey().partition())
        .orElse(0);
  }

  static List<NodeInfo> jmxNodes(Map<Integer, Integer> idAndJmxPort, ClusterInfo clusterInfo) {
    return clusterInfo.nodes().stream()
        .filter(n -> idAndJmxPort.containsKey(n.id()))
        .map(n -> NodeInfo.of(n.id(), n.host(), idAndJmxPort.get(n.id())))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public void configure(Configuration config) {
    idAndJmxPort = config.map(JMX_SERVERS_KEY, ",", ":", Integer::valueOf, Integer::valueOf);
  }

  @Override
  public void close() {
    if (receivers != null) {
      receivers.values().forEach(Utils::close);
      receivers = null;
    }
  }
}
