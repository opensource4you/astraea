package org.astraea.cost;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.kafka.HasValue;
import org.astraea.metrics.kafka.KafkaMetrics;

public class NumberOfLeaderCost implements HasBrokerCost {
  @Override
  public Fetcher fetcher() {
    return client ->
        new java.util.ArrayList<>(KafkaMetrics.ReplicaManager.LeaderCount.fetch(client));
  }

  /**
   * @param clusterInfo cluster information
   * @return a BrokerCost contain all ratio of leaders that exist on all brokers
   */
  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
    Map<Integer, Integer> leaderCount = new HashMap<>();
    Map<Integer, Double> leaderCost = new HashMap<>();
    clusterInfo
        .allBeans()
        .forEach(
            (key, value) ->
                value.stream()
                    .filter(x -> x instanceof HasValue)
                    .filter(x -> x.beanObject().getProperties().get("name").equals("LeaderCount"))
                    .filter(
                        x -> x.beanObject().getProperties().get("type").equals("ReplicaManager"))
                    .sorted(Comparator.comparing(HasBeanObject::createdTimestamp).reversed())
                    .map(x -> (HasValue) x)
                    .limit(1)
                    .forEach(hasValue -> leaderCount.put(key, (int) hasValue.value())));
    var totalLeader = leaderCount.values().stream().mapToInt(Integer::intValue).sum();
    leaderCount.forEach(
        (broker, leaderNum) -> {
          leaderCost.put(broker, (double) leaderNum / totalLeader);
        });
    return () -> leaderCost;
  }
}
