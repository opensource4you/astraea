package org.astraea.partitioner;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.metrics.collector.BeanCollector;
import org.astraea.metrics.collector.Receiver;
import org.astraea.partitioner.cost.CostFunction;

public class StrictCostDispatcher implements Dispatcher {
  public static final String JMX_SERVERS_KEY = "jmx_servers";

  private final BeanCollector beanCollector =
      BeanCollector.builder().interval(Duration.ofSeconds(4)).build();
  private final Collection<CostFunction> functions = List.of(CostFunction.throughput());
  private final List<Receiver> receivers = new ArrayList<>();

  @Override
  public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
    var partitions = clusterInfo.availablePartitions(topic);
    // just return first partition if there is no available partitions
    if (partitions.isEmpty()) return 0;

    // get latest beans for each node
    var beans =
        receivers.stream()
            .collect(
                Collectors.toMap(r -> clusterInfo.node(r.host(), r.port()), Receiver::current));

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

  @Override
  public void configure(Configuration config) {
    receivers.addAll(
        config.map(JMX_SERVERS_KEY, ",", ":", Integer::valueOf).entrySet().stream()
            .map(
                e ->
                    beanCollector
                        .register()
                        .host(e.getKey())
                        .port(e.getValue())
                        .metricsGetters(
                            functions.stream()
                                .flatMap(c -> c.metricsGetters().stream())
                                .collect(Collectors.toUnmodifiableList()))
                        .build())
            .collect(Collectors.toUnmodifiableList()));
  }

  @Override
  public void close() {}
}
