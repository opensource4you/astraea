package org.astraea.cost;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.Utils;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.java.HasJvmMemory;
import org.astraea.metrics.kafka.KafkaMetrics;

/** Warning on heap usage >= 80%. */
public class MemoryWarningCost implements CostFunction {
  private final long constructTime = System.currentTimeMillis();

  /** @return 1 if the heap usage >= 80%, 0 otherwise. */
  @Override
  public ClusterCost cost(ClusterInfo clusterInfo) {
    var brokerScore =
        clusterInfo.allBeans().entrySet().stream()
            .map(
                e ->
                    Map.entry(
                        e.getKey(),
                        e.getValue().stream()
                            .filter(beanObject -> beanObject instanceof HasJvmMemory)
                            .map(beanObject -> (HasJvmMemory) beanObject)
                            .map(
                                hasJvmMemory -> {
                                  if (Utils.overSecond(constructTime, 10)
                                      && (hasJvmMemory.heapMemoryUsage().getUsed() + 0.0)
                                              / (hasJvmMemory.heapMemoryUsage().getMax() + 1)
                                          >= 0.8) {
                                    return 1.0;
                                  } else {
                                    return 0.0;
                                  }
                                })
                            .findAny()
                            .orElseThrow()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var allPartitions =
        clusterInfo.topics().stream()
            .flatMap(topic -> clusterInfo.availablePartitions(topic).stream())
            .collect(Collectors.toList());
    return ClusterCost.scoreByBroker(allPartitions, brokerScore);
  }

  @Override
  public Fetcher fetcher() {
    return client -> List.of(KafkaMetrics.Host.jvmMemory(client));
  }
}
