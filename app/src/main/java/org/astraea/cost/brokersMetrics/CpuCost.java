package org.astraea.cost.brokersMetrics;

import static org.astraea.cost.brokersMetrics.CostUtils.TScore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.Utils;
import org.astraea.cost.BrokerCost;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.HasBrokerCost;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.java.OperatingSystemInfo;
import org.astraea.metrics.kafka.KafkaMetrics;

/**
 * The result is computed by "OperatingSystemInfo.systemCpuLoad".
 *
 * <ol>
 *   <li>We normalize the metric as score(by T-score).
 *   <li>We record these data of each second.
 *   <li>We only keep the last ten seconds of data.
 *   <li>The final result is the average of the ten-second data.
 * </ol>
 */
public class CpuCost implements HasBrokerCost {
  private final Map<Integer, BrokerMetric> brokersMetric = new HashMap<>();
  private long lastFetchTime = 0L;
  private Map<Integer, Double> currentLoad;

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
    if (Utils.overSecond(lastFetchTime, 1)) {
      try {
        var costMetrics =
            clusterInfo.allBeans().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> 0.0));
        clusterInfo
            .allBeans()
            .forEach(
                (brokerID, value) -> {
                  if (!brokersMetric.containsKey(brokerID)) {
                    brokersMetric.put(brokerID, new BrokerMetric(brokerID));
                  }
                  value.stream()
                      .filter(beanObject -> beanObject instanceof OperatingSystemInfo)
                      .forEach(
                          hasBeanObject -> {
                            var cpuBean = (OperatingSystemInfo) hasBeanObject;
                            costMetrics.put(brokerID, cpuBean.systemCpuLoad());
                          });
                });
        TScore(costMetrics).forEach((broker, v) -> brokersMetric.get(broker).updateLoad(v));
        currentLoad = computeLoad();
      } finally {
        lastFetchTime = System.currentTimeMillis();
      }
    }
    return () -> currentLoad;
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
    return client -> List.of(KafkaMetrics.Host.jvmMemory(client));
  }

  private static class BrokerMetric {
    private final int brokerID;

    // mbean data.CpuUsage
    // Record the latest 10 numbers only.
    private final List<Double> load =
        IntStream.range(0, 10).mapToObj(i -> -1.0).collect(Collectors.toList());
    private int loadIndex = 0;

    BrokerMetric(int brokerID) {
      this.brokerID = brokerID;
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
