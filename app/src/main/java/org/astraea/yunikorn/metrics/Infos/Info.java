package org.astraea.yunikorn.metrics.Infos;

import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.astraea.yunikorn.config.NodeSortingPolicy;

@Getter
@Setter
public class Info extends Collector {
  private static final String RUNNING_APPS = "runningApps";
  private static final String COMPLETED_APPS = "completedApps";

  private static final String DELAY_TIME = "delayTime";
  private static final String MIN_DELAY_TIME = "minDelayTime";
  private static final String MAX_DELAY_TIME = "maxDelayTime";
  private static final String AVERAGE_MEMORY = "averageMemory";
  private static final String MEMORY_UTILIZATION = "memoryUtilization";
  private static final String VCORE_UTILIZATION = "vcoreUtilization";
  public static final String NODEID = "nodeID";
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
  private Map<String, BigInteger> capacity = new HashMap<>();
  private Map<String, Double> source = new HashMap<>();
  private List<Application> apps = new ArrayList<>();
  private long runningApps;
  private long delayTime;

  private long completedApps;

  public Info() {
    Map<String, BigInteger> tmp = new HashMap<>();
    tmp.put(NodeSortingPolicy.CORE_KEY, BigInteger.ZERO);
    tmp.put(NodeSortingPolicy.MEMORY_KEY, BigInteger.ZERO);
    averageSource.putAll(tmp);
    maxSource.putAll(tmp);
    minSource.putAll(tmp);
    SDSource.putAll(tmp);
    capacity.putAll(tmp);
    source.put(NodeSortingPolicy.CORE_KEY, 0.0);
    source.put(NodeSortingPolicy.MEMORY_KEY, 0.0);
  }

  public void update(List<Node> nodes, List<Application> apps) throws IOException {
    var capacityCore =
        nodes.stream()
            .map(node -> node.getCapacity().get(NodeSortingPolicy.CORE_KEY))
            .reduce(BigInteger.ZERO, (n1, n2) -> n1.add(n2));
    var capacityMemory =
        nodes.stream()
            .map(node -> node.getCapacity().get(NodeSortingPolicy.MEMORY_KEY))
            .reduce(BigInteger.ZERO, (n1, n2) -> n1.add(n2));
    capacity.put(NodeSortingPolicy.CORE_KEY, capacityCore);
    capacity.put(NodeSortingPolicy.MEMORY_KEY, capacityMemory);

    this.nodes = nodes;
    var core =
        nodes.stream()
            .map(node -> node.getAvailable().get(NodeSortingPolicy.CORE_KEY))
            .reduce(BigInteger.ZERO, (n1, n2) -> n1.add(n2));
    var averageCore = core.divide(BigInteger.valueOf(nodes.stream().count()));
    var memory =
        nodes.stream()
            .map(node -> node.getAvailable().get(NodeSortingPolicy.MEMORY_KEY))
            .reduce(BigInteger.ZERO, (n1, n2) -> n1.add(n2));
    var averageMemory = memory.divide(BigInteger.valueOf(nodes.stream().count()));

    averageSource.put(NodeSortingPolicy.CORE_KEY, averageCore);
    averageSource.put(NodeSortingPolicy.MEMORY_KEY, averageMemory);
    source.put(
        NodeSortingPolicy.CORE_KEY, (1 - core.doubleValue() / capacityCore.doubleValue()) * 100);
    source.put(
        NodeSortingPolicy.MEMORY_KEY,
        (1 - memory.doubleValue() / capacityMemory.doubleValue()) * 100);

    var maxCore =
        nodes.stream()
            .map(node -> node.getAvailable().get(NodeSortingPolicy.CORE_KEY))
            .max(BigInteger::compareTo)
            .get();
    var maxMemory =
        nodes.stream()
            .map(node -> node.getAvailable().get(NodeSortingPolicy.MEMORY_KEY))
            .max(BigInteger::compareTo)
            .get();
    maxSource.put(NodeSortingPolicy.CORE_KEY, maxCore);
    maxSource.put(NodeSortingPolicy.MEMORY_KEY, maxMemory);
    var minCore =
        nodes.stream()
            .map(node -> node.getAvailable().get(NodeSortingPolicy.CORE_KEY))
            .min(BigInteger::compareTo)
            .get();
    var minMemory =
        nodes.stream()
            .map(node -> node.getAvailable().get(NodeSortingPolicy.MEMORY_KEY))
            .min(BigInteger::compareTo)
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
    this.apps = apps;
    this.runningApps =
        apps.stream().filter(app -> app.getApplicationState().compareTo("Running") == 0).count();

    completedApps =
        Long.valueOf(
            apps.stream()
                .filter(app -> app.getApplicationState().compareTo("Completed") == 0)
                .count());
    if (completedApps != 0) {
      this.delayTime =
          apps.stream()
                  .filter(app -> app.getApplicationState().compareTo("Completed") == 0)
                  .map(app -> app.getFinishedTime() - app.getSubmissionTime())
                  .reduce(Long.valueOf(0), (total, app) -> total + app)
              / completedApps;
    }
    writeToCsv();
    printInfo();
  }

  public void printInfo() {
    System.out.println("===================================");
    System.out.printf("running applications: %s\n", String.valueOf(runningApps));
    System.out.printf("completed applications: %s\n", String.valueOf(completedApps));
    System.out.printf(
        "vcore utilization: %s\n", String.valueOf(source.get(NodeSortingPolicy.CORE_KEY)));
    System.out.printf(
        "memory utilization: %s\n", String.valueOf(source.get(NodeSortingPolicy.MEMORY_KEY)));
    System.out.println("===================================");
  }

  @Override
  public List<MetricFamilySamples> collect() {
    List<MetricFamilySamples> mfs = new ArrayList<>();

    mfs.add(new GaugeMetricFamily(COMPLETED_APPS, "help", completedApps));
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
    mfs.add(
        new GaugeMetricFamily(
            MEMORY_UTILIZATION, "help", source.get(NodeSortingPolicy.MEMORY_KEY)));
    mfs.add(
        new GaugeMetricFamily(VCORE_UTILIZATION, "help", source.get(NodeSortingPolicy.CORE_KEY)));
    mfs.add(new GaugeMetricFamily(RUNNING_APPS, "help", runningApps));

    mfs.add(new GaugeMetricFamily(DELAY_TIME, "help", delayTime));

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

  private void writeToCsv() throws IOException {
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd");
    File f = new File(dtf.format(LocalDateTime.now()) + ".csv");
    if (!f.exists()) {
      FileWriter fw = new FileWriter(dtf.format(LocalDateTime.now()) + ".csv", true);
      BufferedWriter bufw = new BufferedWriter(fw);
      bufw.append(VCORE_UTILIZATION)
          .append(',')
          .append(MEMORY_UTILIZATION)
          .append(',')
          .append(RUNNING_APPS)
          .append(',')
          .append(COMPLETED_APPS);
      bufw.newLine();
      bufw.flush();
      bufw.close();
    }
    FileWriter fw = new FileWriter(dtf.format(LocalDateTime.now()) + ".csv", true);
    BufferedWriter bufw = new BufferedWriter(fw);
    bufw.append(source.get(NodeSortingPolicy.CORE_KEY).toString())
        .append(',')
        .append(source.get(NodeSortingPolicy.MEMORY_KEY).toString())
        .append(',')
        .append(String.valueOf(runningApps))
        .append(',')
        .append(String.valueOf(completedApps));
    bufw.newLine();
    bufw.flush();
    bufw.close();
  }
}
