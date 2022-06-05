package org.astraea.cost.broker;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.FakeClusterInfo;
import org.astraea.cost.Normalizer;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.kafka.BrokerTopicMetricsResult;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class BrokerInputCostTest {
  @Test
  void testTScoreCost() {
    ClusterInfo clusterInfo = exampleClusterInfo(50000L, 20000L, 5000L);

    var brokerInputCost = new BrokerInputCost();
    var scores = brokerInputCost.brokerCost(clusterInfo).normalize(Normalizer.TScore()).value();
    Assertions.assertEquals(0.63, scores.get(1));
    Assertions.assertEquals(0.47, scores.get(2));
    Assertions.assertEquals(0.39, scores.get(3));

    ClusterInfo clusterInfo2 = exampleClusterInfo(55555L, 25352L, 25000L);
    scores = brokerInputCost.brokerCost(clusterInfo2).normalize(Normalizer.TScore()).value();
    Assertions.assertEquals(0.64, scores.get(1));
    Assertions.assertEquals(0.43, scores.get(2));
    Assertions.assertEquals(0.43, scores.get(3));
  }

  @Test
  void testNoNormalize() {
    ClusterInfo clusterInfo = exampleClusterInfo(10000L, 20000L, 5000L);

    var brokerInputCost = new BrokerInputCost();
    var scores = brokerInputCost.brokerCost(clusterInfo).value();
    Assertions.assertEquals(10000.0, scores.get(1));
    Assertions.assertEquals(20000.0, scores.get(2));
    Assertions.assertEquals(5000.0, scores.get(3));

    ClusterInfo clusterInfo2 = exampleClusterInfo(55555L, 25352L, 25000L);
  }

  private ClusterInfo exampleClusterInfo(long in1, long in2, long in3) {
    var BytesInPerSec1 = mockResult(KafkaMetrics.BrokerTopic.BytesInPerSec.metricName(), in1);
    var BytesInPerSec2 = mockResult(KafkaMetrics.BrokerTopic.BytesInPerSec.metricName(), in2);
    var BytesInPerSec3 = mockResult(KafkaMetrics.BrokerTopic.BytesInPerSec.metricName(), in3);

    Collection<HasBeanObject> broker1 = List.of(BytesInPerSec1);
    Collection<HasBeanObject> broker2 = List.of(BytesInPerSec2);
    Collection<HasBeanObject> broker3 = List.of(BytesInPerSec3);
    return new FakeClusterInfo() {
      @Override
      public Map<Integer, Collection<HasBeanObject>> allBeans() {
        return Map.of(1, broker1, 2, broker2, 3, broker3);
      }
    };
  }

  private BrokerTopicMetricsResult mockResult(String name, long count) {
    var result = Mockito.mock(BrokerTopicMetricsResult.class);
    var bean = Mockito.mock(BeanObject.class);
    Mockito.when(result.beanObject()).thenReturn(bean);
    Mockito.when(bean.getProperties()).thenReturn(Map.of("name", name));
    Mockito.when(result.count()).thenReturn(count);
    return result;
  }
}
