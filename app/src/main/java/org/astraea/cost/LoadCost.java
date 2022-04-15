package org.astraea.cost;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.kafka.BrokerTopicMetricsResult;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.astraea.partitioner.nodeLoadMetric.PartitionerUtils;

public class LoadCost implements CostFunction {
  private final Map<Integer, BrokerMetric> brokersMetric = new HashMap<>();
  private final Map<String, Double> metricNameAndWeight =
      Map.of(
          KafkaMetrics.BrokerTopic.BytesInPerSec.metricName(),
          0.5,
          KafkaMetrics.BrokerTopic.BytesOutPerSec.metricName(),
          0.5);

  /** Do "Poisson" and "weightPoisson" calculation on "load". And change output to double. */
  @Override
  public Map<TopicPartitionReplica, Double> cost(ClusterInfo clusterInfo) {
    var load = computeLoad(clusterInfo.allBeans());

    // Poisson calculation (-> Poisson -> throughputAbility -> to double)
    var brokerScore =
        PartitionerUtils.allPoisson(load).entrySet().stream()
            .map(e -> Map.entry(e.getKey(), PartitionerUtils.weightPoisson(e.getValue(), 1.0)))
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (double) e.getValue()));
    return clusterInfo.topics().stream()
        .flatMap(topic -> clusterInfo.availablePartitions(topic).stream())
        .map(
            p ->
                Map.entry(
                    PartitionInfo.leaderReplica(p), brokerScore.getOrDefault(p.leader().id(), 1.0)))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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
  Map<Integer, Integer> computeLoad(Map<Integer, Collection<HasBeanObject>> allBeans) {
    // Store mbean data into local stat("brokersMetric").
    allBeans.forEach(
        (brokerID, value) -> {
          if (!brokersMetric.containsKey(brokerID))
            brokersMetric.put(brokerID, new BrokerMetric(brokerID));
          value.stream()
              .filter(hasBeanObject -> hasBeanObject instanceof BrokerTopicMetricsResult)
              .map(hasBeanObject -> (BrokerTopicMetricsResult) hasBeanObject)
              .forEach(
                  result ->
                      brokersMetric
                          .get(brokerID)
                          .updateCount(
                              result.beanObject().getProperties().get("name"), result.count()));
        });

    // Sum all count with the same mbean name
    var total =
        brokersMetric.values().stream()
            .map(brokerMetric -> brokerMetric.currentCount)
            .reduce(
                new HashMap<>(),
                (accumulate, currentCount) -> {
                  currentCount.forEach(
                      (name, count) ->
                          accumulate.put(name, accumulate.getOrDefault(name, 0L) + count));
                  return accumulate;
                });

    // Reduce all count for all brokers' mbean current count by "divide total" (current/total). And
    // then get the weighted sum according to predefined wights "metricNameAndWeight".
    // It is called "brokerSituation" in original name.
    var weightedSum =
        brokersMetric.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        entry.getValue().currentCount.entrySet().stream()
                            .mapToDouble(
                                nameAndCount ->
                                    (double) (nameAndCount.getValue() + 1)
                                        / (total.getOrDefault(nameAndCount.getKey(), 1L) + 1)
                                        * metricNameAndWeight.getOrDefault(
                                            nameAndCount.getKey(), 0.0))
                            .sum()));

    var avgWeightedSum =
        weightedSum.values().stream().mapToDouble(d -> d).sum() / brokersMetric.size();

    // Record the "load" into local stat("brokersMetric").
    weightedSum.forEach(
        (brokerID, theWeightedSum) -> {
          if (theWeightedSum < avgWeightedSum * 0.5) brokersMetric.get(brokerID).updateLoad(0);
          else if (theWeightedSum < avgWeightedSum * 1.5) brokersMetric.get(brokerID).updateLoad(1);
          else brokersMetric.get(brokerID).updateLoad(2);
        });

    // The sum of historical and current load.
    return brokersMetric.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, entry -> entry.getValue().load.stream().mapToInt(i -> i).sum()));
  }

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  @Override
  public Fetcher fetcher() {
    return client ->
        List.of(
            KafkaMetrics.BrokerTopic.BytesInPerSec.fetch(client),
            KafkaMetrics.BrokerTopic.BytesOutPerSec.fetch(client));
  }

  private static class BrokerMetric {
    private final int brokerID;

    // mbean data. They are:
    // ("BytesInPerSec", BytesInPerSec.count), ("BytesOutPerSec", ByteOutPerSec.count)
    private final Map<String, Long> accumulateCount = new HashMap<>();
    private final Map<String, Long> currentCount = new HashMap<>();

    // Record the latest 10 numbers only.
    private final List<Integer> load =
        IntStream.range(0, 10).mapToObj(i -> 0).collect(Collectors.toList());
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
    void updateCount(String domainName, Long count) {
      currentCount.put(domainName, count - accumulateCount.getOrDefault(domainName, 0L));
      accumulateCount.put(domainName, count);
    }

    /**
     * This method record input data into a list. This list contains the latest 10 record. Each time
     * it is called, the current index, "loadIndex", is increased by 1.
     */
    void updateLoad(Integer load) {
      this.load.set(loadIndex, load);
      loadIndex = (loadIndex + 1) % 10;
    }
  }
}
