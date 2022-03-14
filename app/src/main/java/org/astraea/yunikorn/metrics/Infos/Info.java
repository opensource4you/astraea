package org.astraea.yunikorn.metrics.Infos;

import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.astraea.yunikorn.config.NodeSortingPolicy;

@Getter
@Setter
public class Info extends Collector {
  private static final String AVERAGE_MEMORY = "averageMemory";
  private static final String NODEID = "nodeID";
  private static final String MAX_MEMORY = "maxMemory";
  private static final String MIN_MEMORY = "minMemory";
  private static final String SD_MEMORY = "StandardDeviationMemory";
  private static final String AVERAGE_CORE = "averageCore";
  private static final String MAX_CORE = "maxCore";
  private static final String MIN_CORE = "minCore";
  private static final String SD_CORE = "StandardDeviationCore";
  private static final String NODES_CORE = "nodesCore";
  private static final String NODES_MEMORY = "nodesMemory";
  private List<Node> nodes = new ArrayList<>();
  private Map<String, BigInteger> averageSource = new HashMap<>();
  private Map<String, BigInteger> maxSource = new HashMap<>();
  private Map<String, BigInteger> minSource = new HashMap<>();
  private Map<String, BigInteger> SDSource = new HashMap<>();

  public Info() {
    Map<String, BigInteger> tmp = new HashMap<>();
    tmp.put(NodeSortingPolicy.CORE_KEY, BigInteger.ZERO);
    tmp.put(NodeSortingPolicy.MEMORY_KEY, BigInteger.ZERO);
    averageSource.putAll(tmp);
    maxSource.putAll(tmp);
    minSource.putAll(tmp);
    SDSource.putAll(tmp);
  }

  public void update(List<Node> nodes) {
    this.nodes = nodes;
    var averageCore =
        nodes.stream()
            .map(node -> node.getAvailable().get(NodeSortingPolicy.CORE_KEY))
            .reduce(BigInteger.ZERO, (n1, n2) -> n1.add(n2))
            .divide(BigInteger.valueOf(nodes.stream().count()));
    var averageMemory =
        nodes.stream()
            .map(node -> node.getAvailable().get(NodeSortingPolicy.MEMORY_KEY))
            .reduce(BigInteger.ZERO, (n1, n2) -> n1.add(n2))
            .divide(BigInteger.valueOf(nodes.stream().count()));

    averageSource.put(NodeSortingPolicy.CORE_KEY, averageCore);
    averageSource.put(NodeSortingPolicy.MEMORY_KEY, averageMemory);
    var maxCore =
        nodes.stream()
            .map(node -> node.getAvailable().get(NodeSortingPolicy.CORE_KEY))
            .max(Comparator.comparing(BigInteger::toString))
            .get();
    var maxMemory =
        nodes.stream()
            .map(node -> node.getAvailable().get(NodeSortingPolicy.MEMORY_KEY))
            .max(Comparator.comparing(BigInteger::toString))
            .get();
    maxSource.put(NodeSortingPolicy.CORE_KEY, maxCore);
    maxSource.put(NodeSortingPolicy.MEMORY_KEY, maxMemory);
    var minCore =
        nodes.stream()
            .map(node -> node.getAvailable().get(NodeSortingPolicy.CORE_KEY))
            .max(Comparator.comparing(BigInteger::toString))
            .get();
    var minMemory =
        nodes.stream()
            .map(node -> node.getAvailable().get(NodeSortingPolicy.MEMORY_KEY))
            .max(Comparator.comparing(BigInteger::toString))
            .get();
    minSource.put(NodeSortingPolicy.MEMORY_KEY, minMemory);
    minSource.put(NodeSortingPolicy.CORE_KEY, minCore);
    var SBCore =
        nodes.stream()
            .map(node -> node.getAvailable().get(NodeSortingPolicy.CORE_KEY))
            .map(bigInteger -> bigInteger.subtract(averageCore))
            .map(bigInteger -> bigInteger.multiply(bigInteger))
            .reduce(BigInteger.ZERO, (bigInteger, bigInteger2) -> bigInteger.add(bigInteger2))
            .divide(BigInteger.valueOf(nodes.stream().count()))
            .sqrt();
    var SBMemory =
        nodes.stream()
            .map(node -> node.getAvailable().get(NodeSortingPolicy.MEMORY_KEY))
            .map(bigInteger -> bigInteger.subtract(averageMemory))
            .map(bigInteger -> bigInteger.multiply(bigInteger))
            .reduce(BigInteger.ZERO, (bigInteger, bigInteger2) -> bigInteger.add(bigInteger2))
            .divide(BigInteger.valueOf(nodes.stream().count()))
            .sqrt();
    SDSource.put(NodeSortingPolicy.CORE_KEY, SBCore);
    SDSource.put(NodeSortingPolicy.MEMORY_KEY, SBMemory);
  }

  @Override
  public List<MetricFamilySamples> collect() {
    List<MetricFamilySamples> mfs = new ArrayList<>();

    // With no labels.
    mfs.add(
        new GaugeMetricFamily(
            MAX_CORE, "help", maxSource.get(NodeSortingPolicy.CORE_KEY).doubleValue()));
    mfs.add(
        new GaugeMetricFamily(
            MAX_MEMORY, "help", maxSource.get(NodeSortingPolicy.MEMORY_KEY).doubleValue()));
    mfs.add(
        new GaugeMetricFamily(
            MIN_CORE, "help", minSource.get(NodeSortingPolicy.CORE_KEY).doubleValue()));
    mfs.add(
        new GaugeMetricFamily(
            MIN_MEMORY, "help", minSource.get(NodeSortingPolicy.MEMORY_KEY).doubleValue()));
    mfs.add(
        new GaugeMetricFamily(
            AVERAGE_CORE, "help", averageSource.get(NodeSortingPolicy.CORE_KEY).doubleValue()));
    mfs.add(
        new GaugeMetricFamily(
            AVERAGE_MEMORY, "help", averageSource.get(NodeSortingPolicy.MEMORY_KEY).doubleValue()));
    mfs.add(
        new GaugeMetricFamily(
            SD_CORE, "help", SDSource.get(NodeSortingPolicy.CORE_KEY).doubleValue()));
    mfs.add(
        new GaugeMetricFamily(
            SD_MEMORY, "help", SDSource.get(NodeSortingPolicy.MEMORY_KEY).doubleValue()));

    // With labels
    GaugeMetricFamily labeledGaugeCore =
        new GaugeMetricFamily(NODES_CORE, "help", Arrays.asList(NODEID));
    nodes.stream()
        .forEach(
            node ->
                labeledGaugeCore.addMetric(
                    Arrays.asList(node.getNodeID()),
                    node.getAvailable().get(NodeSortingPolicy.CORE_KEY).doubleValue()));
    mfs.add(labeledGaugeCore);
    GaugeMetricFamily labeledGaugeMemory =
        new GaugeMetricFamily(NODES_MEMORY, "help", Arrays.asList(NODEID));
    nodes.stream()
        .forEach(
            node ->
                labeledGaugeMemory.addMetric(
                    Arrays.asList(node.getNodeID()),
                    node.getAvailable().get(NodeSortingPolicy.MEMORY_KEY).doubleValue()));
    mfs.add(labeledGaugeMemory);

    return mfs;
  }
}
