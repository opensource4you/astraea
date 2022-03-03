package org.astraea.partitioner.cost;

import java.util.List;
import java.util.Map;
import org.astraea.metrics.kafka.BrokerTopicMetricsResult;
import org.astraea.partitioner.ClusterInfo;
import org.astraea.partitioner.NodeInfo;
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

    var cost = throughputCost.cost(Map.of(10, List.of(bean)), cluster);
    Assertions.assertEquals(1, cost.size());
    Assertions.assertEquals(1, cost.get(10));
  }
}
