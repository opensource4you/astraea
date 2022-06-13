package org.astraea.cost;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.admin.BeansGetter;
import org.astraea.metrics.kafka.BrokerTopicMetricsResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ThroughputCostTest {

  @Test
  void testScore() {
    var throughputCost = new ThroughputCost();

    var node = NodeInfo.of(10, "host", 1000);
    var bean = Mockito.mock(BrokerTopicMetricsResult.class);
    Mockito.when(bean.oneMinuteRate()).thenReturn(100D);

    var score = throughputCost.score(Map.of(10, List.of(bean)));
    Assertions.assertEquals(1, score.size());
    Assertions.assertEquals(100D, score.get(10));
  }

  @Test
  void testCost() {
    var throughputCost = new ThroughputCost();

    var node = NodeInfo.of(10, "host", 1000);
    var bean = Mockito.mock(BrokerTopicMetricsResult.class);
    Mockito.when(bean.oneMinuteRate()).thenReturn(100D);

    var cluster = Mockito.mock(ClusterInfo.class);
    Mockito.when(cluster.nodes()).thenReturn(List.of(node));
    Mockito.when(cluster.beans()).thenReturn(BeansGetter.of(Map.of()));
    Mockito.when(cluster.topics()).thenReturn(Set.of("t"));
    Mockito.when(cluster.availableReplicas("t"))
        .thenReturn(List.of(ReplicaInfo.of("t", 0, node, true, true, false)));

    var cost =
        throughputCost.brokerCost(ClusterInfo.of(cluster, Map.of(10, List.of(bean)))).value();
    Assertions.assertEquals(1, cost.size());
    Assertions.assertEquals(1, cost.get(10));
  }
}
