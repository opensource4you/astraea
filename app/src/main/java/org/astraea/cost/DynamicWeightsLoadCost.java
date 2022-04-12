package org.astraea.cost;

import static org.astraea.cost.CostUtils.TScore;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.Utils;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.astraea.partitioner.smoothPartitioner.DynamicWeights;
import org.astraea.partitioner.smoothPartitioner.DynamicWeightsMetrics;

public class DynamicWeightsLoadCost implements CostFunction {
  private final Map<String, HashMap<Integer, BrokerMetric>> brokersMetrics =
      Map.of(
          DynamicWeightsMetrics.BrokerMetrics.inputThroughput.metricName(), new HashMap<>(),
          DynamicWeightsMetrics.BrokerMetrics.outputThroughput.metricName(), new HashMap<>(),
          DynamicWeightsMetrics.BrokerMetrics.jvm.metricName(), new HashMap<>(),
          DynamicWeightsMetrics.BrokerMetrics.cpu.metricName(), new HashMap<>());

  private final EntropyEmpowerment entropyEmpowerment = new EntropyEmpowerment();
  private final ANPEmpowerment anpEmpowerment = new ANPEmpowerment();
  private final DynamicWeightsMetrics dynamicWeightsMetrics;
  private final DynamicWeights dynamicWeightsCal = new DynamicWeights();
  private final Lock lock = new ReentrantLock();
  private long lastFetchTime = 0L;

  public DynamicWeightsLoadCost() {
    this.dynamicWeightsMetrics = new DynamicWeightsMetrics();
  }

  DynamicWeightsLoadCost(DynamicWeightsMetrics dynamicWeightsMetrics) {
    this.dynamicWeightsMetrics = dynamicWeightsMetrics;
  }

  /**
   * The result is computed by "BytesInPerSec.count", "BytesOutPerSec.count",
   * "MemoryUsage"and"CpuUsage".
   *
   * <ol>
   *   <li>We normalize each metric (by divide sum of each metric).
   *   <li>We compute the the T-score for each metric.
   *   <li>The average historical "T-score" is multiplied by the factor of each metric and summed to
   *       obtain the load.
   * </ol>
   *
   * @return brokerID with "(sum of load)".
   */
  @Override
  public Map<Integer, Double> cost(ClusterInfo clusterInfo) {
    if (Utils.overSecond(lastFetchTime, 10) && lock.tryLock()) {
      try {
        var compoundScore = computeLoad(clusterInfo.allBeans());
        dynamicWeightsCal.init(compoundScore);
      } finally {
        lastFetchTime = System.currentTimeMillis();
        lock.unlock();
      }
    }
    return dynamicWeightsCal.getLoad();
  }

  @Override
  public void updateLoad(ClusterInfo clusterInfo) {
    dynamicWeightsMetrics.updateMetrics(clusterInfo.allBeans());
    brokersMetrics.forEach(
        (name, brokersMetric) -> {
          if (name.equals(DynamicWeightsMetrics.BrokerMetrics.inputThroughput.metricName())) {
            dynamicWeightsMetrics
                .inputCount()
                .forEach(
                    (brokerID, value) -> {
                      if (!brokersMetric.containsKey(brokerID))
                        brokersMetric.put(brokerID, new BrokerMetric(brokerID));
                      brokersMetric.get(brokerID).updateCount(value);
                    });
          } else if (name.equals(
              DynamicWeightsMetrics.BrokerMetrics.outputThroughput.metricName())) {
            dynamicWeightsMetrics
                .outputCount()
                .forEach(
                    (brokerID, value) -> {
                      if (!brokersMetric.containsKey(brokerID))
                        brokersMetric.put(brokerID, new BrokerMetric(brokerID));
                      brokersMetric.get(brokerID).updateCount(value);
                    });
          } else if (name.equals(DynamicWeightsMetrics.BrokerMetrics.jvm.metricName())) {
            dynamicWeightsMetrics
                .jvmUsage()
                .forEach(
                    (brokerID, value) -> {
                      if (!brokersMetric.containsKey(brokerID))
                        brokersMetric.put(brokerID, new BrokerMetric(brokerID));
                      brokersMetric.get(brokerID).updateCount(value);
                    });
          } else if (name.equals(DynamicWeightsMetrics.BrokerMetrics.cpu.metricName())) {
            dynamicWeightsMetrics
                .cpuUsage()
                .forEach(
                    (brokerID, value) -> {
                      if (!brokersMetric.containsKey(brokerID))
                        brokersMetric.put(brokerID, new BrokerMetric(brokerID));
                      brokersMetric.get(brokerID).updateCount(value);
                    });
          }
        });

    var allMetrics =
        brokersMetrics.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        entry.getValue().entrySet().stream()
                            .collect(
                                Collectors.toMap(
                                    Map.Entry::getKey, e -> e.getValue().currentCount))))
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, em -> TScore(em.getValue())));

    allMetrics.forEach(
        (key, value) -> {
          value.forEach((k, v) -> brokersMetrics.get(key).get(k).updateLoad(v));
        });
  }

  Map<Integer, Double> computeLoad(Map<Integer, Collection<HasBeanObject>> allBeans) {
    var costLoad =
        allBeans.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> 0.0));
    var metricsSum =
        brokersMetrics.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        e.getValue().entrySet().stream()
                            .collect(
                                Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry ->
                                        (entry.getValue().load.stream()
                                            .filter(aDouble -> !aDouble.equals(-1.0))
                                            .mapToDouble(i -> i)
                                            .average()
                                            .getAsDouble())))));
    var costFraction = costFraction(metricsSum);
    metricsSum.forEach(
        (metricName, brokersMe) ->
            brokersMe.forEach(
                (ID, value) ->
                    costLoad.put(
                        ID,
                        (double)
                                Math.round(
                                    100 * (costLoad.get(ID) + value * costFraction.get(metricName)))
                            / 100.0)));

    return costLoad;
  }

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  @Override
  public Fetcher fetcher() {
    return client ->
        List.of(
            KafkaMetrics.BrokerTopic.BytesInPerSec.fetch(client),
            KafkaMetrics.BrokerTopic.BytesOutPerSec.fetch(client),
            KafkaMetrics.Host.jvmMemory(client),
            KafkaMetrics.Host.operatingSystem(client));
  }

  private Map<String, Double> costFraction(Map<String, Map<Integer, Double>> bMetrics) {
    var anp = anpEmpowerment.empowerment(dynamicWeightsMetrics);
    return entropyEmpowerment.empowerment(bMetrics).entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue() * 0.5 + anp.get(entry.getKey()) * 0.5));
  }

  private static class BrokerMetric {
    private final int brokerID;

    private double currentCount = 0.0;

    // Record the latest 10 numbers only.
    private final List<Double> load =
        IntStream.range(0, 10).mapToObj(i -> -1.0).collect(Collectors.toList());
    private int loadIndex = 0;

    BrokerMetric(int brokerID) {
      this.brokerID = brokerID;
    }

    /**
     * This method records the difference between last update and current given "count" e.g.
     *
     * <p>At time 1: updateCount("ByteInPerSec", 100);<br>
     * At time 3: updateCount("ByteOutPerSec", 20);<br>
     * At time 20: updateCount("ByteInPerSec", 150);<br>
     * At time 22: updateCount("ByteOutPerSec", 90);<br>
     * At time 35: updateCount("ByteInPerSec", 170)<br>
     * Then in time [20, 35), currentCount.get("ByteInPerSec") is 50
     *
     * <p>
     */
    void updateCount(double count) {
      currentCount = count;
    }

    /**
     * This method record input data into a list. This list contains the latest 10 record. Each time
     * it is called, the current index, "loadIndex", is increased by 1.
     */
    void updateLoad(Double load) {
      var cLoad = load.isNaN() ? 0.5 : load;
      this.load.set(loadIndex, cLoad);
      loadIndex = (loadIndex + 1) % 10;
    }
  }
}
