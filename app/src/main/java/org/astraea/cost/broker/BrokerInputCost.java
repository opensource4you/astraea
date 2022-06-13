package org.astraea.cost.broker;

import static org.astraea.cost.broker.CostUtils.TScore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.cost.BrokerCost;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.HasBrokerCost;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.kafka.BrokerTopicMetricsResult;
import org.astraea.metrics.kafka.KafkaMetrics;

/**
 * The result is computed by "BytesInPerSec.count". "BytesInPerSec.count" responds to the input
 * throughput of brokers.
 *
 * <ol>
 *   <li>We normalize the metric as score(by T-score).
 *   <li>We record these data of each second.
 *   <li>We only keep the last ten seconds of data.
 *   <li>The final result is the average of the ten-second data.
 * </ol>
 */
public class BrokerInputCost implements HasBrokerCost {
  private final Map<Integer, BrokerMetric> brokersMetric = new HashMap<>();

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
    var costMetrics =
        clusterInfo.beans().broker().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        entry.getValue().stream()
                            .filter(
                                hasBeanObject ->
                                    KafkaMetrics.BrokerTopic.BytesInPerSec.metricName()
                                        .equals(
                                            hasBeanObject.beanObject().getProperties().get("name")))
                            .findAny()
                            .orElseThrow()))
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> {
                      if (!brokersMetric.containsKey(entry.getKey())) {
                        brokersMetric.put(entry.getKey(), new BrokerMetric());
                      }
                      var broker = brokersMetric.get(entry.getKey());
                      var inBean = (BrokerTopicMetricsResult) entry.getValue();
                      var count = (double) (inBean.count() - broker.accumulateCount);
                      broker.accumulateCount = inBean.count();
                      return count;
                    }));

    TScore(costMetrics).forEach((broker, v) -> brokersMetric.get(broker).updateLoad(v));

    return this::computeLoad;
  }

  Map<Integer, Double> computeLoad() {
    return brokersMetric.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e ->
                    Math.round(
                            e.getValue().load.stream()
                                    .filter(aDouble -> !aDouble.equals(-1.0))
                                    .mapToDouble(i -> i)
                                    .average()
                                    .orElse(0.5)
                                * 100)
                        / 100.0));
  }

  @Override
  public Fetcher fetcher() {
    return client -> List.of(KafkaMetrics.BrokerTopic.BytesInPerSec.fetch(client));
  }

  private static class BrokerMetric {
    // mbean data.  BytesInPerSec.count
    private long accumulateCount = 0L;

    // Record the latest 10 numbers only.
    private final List<Double> load =
        IntStream.range(0, 10).mapToObj(i -> -1.0).collect(Collectors.toList());
    private int loadIndex = 0;

    private BrokerMetric() {}

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
