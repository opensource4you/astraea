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
import org.astraea.partitioner.smoothPartitioner.SmoothWeight;
import org.astraea.partitioner.smoothPartitioner.SmoothWeightMetrics;

public class ThroughputLoadCost implements CostFunction {
  private final Map<String, HashMap<Integer, BrokerMetric>> brokersMetrics =
      Map.of(
          SmoothWeightMetrics.BrokerMetrics.inputThroughput.metricName(),
          new HashMap<Integer, BrokerMetric>(),
          SmoothWeightMetrics.BrokerMetrics.outputThroughput.metricName(),
          new HashMap<Integer, BrokerMetric>(),
          SmoothWeightMetrics.BrokerMetrics.jvm.metricName(),
          new HashMap<Integer, BrokerMetric>(),
          SmoothWeightMetrics.BrokerMetrics.cpu.metricName(),
          new HashMap<Integer, BrokerMetric>());
  ;
  //  private final Map<Integer, BrokerMetric> brokersMetric = new HashMap<>();
  private final EntropyEmpowerment entropyEmpowerment = new EntropyEmpowerment();
  private final ANPEmpowerment anpEmpowerment = new ANPEmpowerment();
  private final SmoothWeightMetrics smoothWeightMetrics;
  private final SmoothWeight smoothWeightCal = new SmoothWeight();
  private final Lock lock = new ReentrantLock();
  private long lastFetchTime = 0L;

  public ThroughputLoadCost() {
    this.smoothWeightMetrics = new SmoothWeightMetrics();
  }

  ThroughputLoadCost(SmoothWeightMetrics smoothWeightMetrics) {
    this.smoothWeightMetrics = smoothWeightMetrics;
  }

  @Override
  public SmoothWeightMetrics smoothWeightMetrics() {
    return this.smoothWeightMetrics;
  }

  /** Do "Poisson" and "weightPoisson" calculation on "load". And change output to double. */
  @Override
  public Map<Integer, Double> cost(ClusterInfo clusterInfo) {
    if (Utils.overSecond(lastFetchTime, 10) && lock.tryLock()) {
      try {
        var compoundScore = computeLoad(clusterInfo.allBeans());
        smoothWeightCal.init(compoundScore);
      } finally {
        lastFetchTime = System.currentTimeMillis();
        lock.unlock();
      }
    }
    return smoothWeightCal.getLoad();
  }

  /**
   * The result is computed by "BytesInPerSec.count" and "BytesOutPerSec.count".
   *
   * <ol>
   *   <li>We normalize the two metric (by divide sum of each metric).
   *   <li>We compute the sum on the two metric with a specific weight.
   *   <li>Compare the weighted sum with two boundary(0.5*avg and 1.5*avg) to get the "load" {0,1,2}
   *   <li>Sum up the historical "load" and divide by 20 the get the score.
   * </ol>
   *
   * <p>e.g. We have 3 brokers with information:<br>
   * broker1: BytesInPerSec.count=50, BytesOutPerSec.count=15 (<1,50000,21000>)<br>
   * broker2: BytesInPerSec.count=100, BytesOutPerSec.count=2 (<2,100000,2000>)<br>
   * broker3: BytesInPerSec.count=200, BytesOutPerSec.count=1 (<3,200000,1000>)<br>
   *
   * <ol>
   *   <li>Normalize: about <1, 1/7, 7/8> <2, 2/7, 1/12> <3, 4/7, 1/24>
   *   <li>WeightedSum: about <1, 57/112> <2, 31/168> <3, 103/336>
   *   <li>Load:<1,2><2,1><3,1>
   * </ol>
   *
   * broker1 score: 2<br>
   * broker2 score: 1<br>
   * broker3 score: 1<br>
   *
   * @return brokerID with "(sum of load)".
   */
  @Override
  public void updateLoad(ClusterInfo clusterInfo) {
    smoothWeightMetrics.updateMetrics(clusterInfo.allBeans());
    brokersMetrics.forEach(
        (name, brokersMetric) -> {
          if (name.equals(SmoothWeightMetrics.BrokerMetrics.inputThroughput.metricName())) {
            smoothWeightMetrics
                .inputCount()
                .forEach(
                    (brokerID, value) -> {
                      if (!brokersMetric.containsKey(brokerID))
                        brokersMetric.put(brokerID, new BrokerMetric(brokerID));
                      brokersMetric.get(brokerID).updateCount(value);
                    });
          } else if (name.equals(SmoothWeightMetrics.BrokerMetrics.outputThroughput.metricName())) {
            smoothWeightMetrics
                .outputCount()
                .forEach(
                    (brokerID, value) -> {
                      if (!brokersMetric.containsKey(brokerID))
                        brokersMetric.put(brokerID, new BrokerMetric(brokerID));
                      brokersMetric.get(brokerID).updateCount(value);
                    });
          } else if (name.equals(SmoothWeightMetrics.BrokerMetrics.jvm.metricName())) {
            smoothWeightMetrics
                .jvmUsage()
                .forEach(
                    (brokerID, value) -> {
                      if (!brokersMetric.containsKey(brokerID))
                        brokersMetric.put(brokerID, new BrokerMetric(brokerID));
                      brokersMetric.get(brokerID).updateCount(value);
                    });
          } else if (name.equals(SmoothWeightMetrics.BrokerMetrics.cpu.metricName())) {
            smoothWeightMetrics
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

    // Reduce all count for all brokers' mbean current count by "divide total" (current/total). And
    // then get the weighted sum according to predefined wights "metricNameAndWeight".
    // It is called "brokerSituation" in original name.
    //    var throughPut =
    //        brokersMetric.entrySet().stream()
    //            .collect(Collectors.toMap(Map.Entry::getKey, entry ->
    // entry.getValue().currentCount));
    //
    //    var tScore = TScore(throughPut);

    //    tScore.forEach((key, value) -> brokersMetric.get(key).updateLoad(value));
  }

  Map<Integer, Double> computeLoad(Map<Integer, Collection<HasBeanObject>> allBeans) {
    var costLoad =
        allBeans.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> 0.0));
    var test =
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
                                        Math.round(
                                                (entry.getValue().load.stream()
                                                        .filter(aDouble -> !aDouble.equals(-1.0))
                                                        .mapToDouble(i -> i)
                                                        .average()
                                                        .getAsDouble())
                                                    * 100)
                                            / 100.0))));
    var costFraction = costFraction(test);
    //    System.out.println("costFraction" + costFraction);
    //    System.out.println("test" + test);
    test.forEach(
        (metricName, brokersMe) ->
            brokersMe.forEach(
                (ID, value) ->
                    costLoad.put(ID, costLoad.get(ID) + value * costFraction.get(metricName))));

    // The sum of historical and current load.
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
    var anp = anpEmpowerment.empowerment(smoothWeightMetrics);
    //    System.out.println("anp:" + anp);
    var ent = entropyEmpowerment.empowerment(bMetrics);
    //    System.out.println("ent:" + ent);
    return entropyEmpowerment.empowerment(bMetrics).entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue() * 0.5 + anp.get(entry.getKey()) * 0.5));
  }

  private static class BrokerMetric {
    private final int brokerID;

    // mbean data. They are:
    // ("BytesInPerSec", BytesInPerSec.count), ("BytesOutPerSec", ByteOutPerSec.count)
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
      this.load.set(loadIndex, load);
      loadIndex = (loadIndex + 1) % 10;
    }
  }
}
