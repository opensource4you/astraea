package org.astraea.cost.brokersMetrics;

import static org.astraea.cost.brokersMetrics.CostUtils.TScore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.CostFunction;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.java.HasJvmMemory;
import org.astraea.metrics.kafka.KafkaMetrics;

public class JvmCost implements CostFunction {
  private final Map<Integer, BrokerMetric> brokersMetric = new HashMap<>();

  @Override
  public Map<Integer, Double> cost(ClusterInfo clusterInfo) {
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
                  .filter(beanObject -> beanObject instanceof HasJvmMemory)
                  .forEach(
                      hasBeanObject -> {
                        var broker = brokersMetric.get(brokerID);
                        var jvmBean = (HasJvmMemory) hasBeanObject;
                        costMetrics.put(
                            brokerID,
                            ((jvmBean.heapMemoryUsage().getUsed() + 0.0)
                                / (jvmBean.heapMemoryUsage().getMax() + 1)));
                      });
              TScore(costMetrics).forEach((broker, v) -> brokersMetric.get(broker).updateLoad(v));
            });
    return computeLoad();
  }

  Map<Integer, Double> computeLoad() {
    return brokersMetric.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e ->
                    (e.getValue().load.stream()
                        .filter(aDouble -> !aDouble.equals(-1.0))
                        .mapToDouble(i -> i)
                        .average()
                        .getAsDouble())));
  }

  @Override
  public Fetcher fetcher() {
    return client -> List.of(KafkaMetrics.Host.jvmMemory(client));
  }

  private static class BrokerMetric {
    private final int brokerID;

    // mbean data.JvmUsage
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
