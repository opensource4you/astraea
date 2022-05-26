package org.astraea.cost.broker;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.FakeClusterInfo;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.kafka.HasValue;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class NumberOfLeaderCostTest {

  @Test
  void testBrokerCost() {
    var loadCostFunction = new NumberOfLeaderCost();
    var load = loadCostFunction.brokerCost(exampleClusterInfo());

    Assertions.assertEquals(3.0 / (3 + 4 + 5), load.value().get(1));
    Assertions.assertEquals(4.0 / (3 + 4 + 5), load.value().get(2));
    Assertions.assertEquals(5.0 / (3 + 4 + 5), load.value().get(3));
  }

  private ClusterInfo exampleClusterInfo() {
    var LeaderCount1 =
        mockResult("ReplicaManager", KafkaMetrics.ReplicaManager.LeaderCount.metricName(), 3);
    var LeaderCount2 =
        mockResult("ReplicaManager", KafkaMetrics.ReplicaManager.LeaderCount.metricName(), 4);
    var LeaderCount3 =
        mockResult("ReplicaManager", KafkaMetrics.ReplicaManager.LeaderCount.metricName(), 5);

    Collection<HasBeanObject> broker1 = List.of(LeaderCount1);
    Collection<HasBeanObject> broker2 = List.of(LeaderCount2);
    Collection<HasBeanObject> broker3 = List.of(LeaderCount3);
    return new FakeClusterInfo() {
      @Override
      public Map<Integer, Collection<HasBeanObject>> allBeans() {
        return Map.of(1, broker1, 2, broker2, 3, broker3);
      }
    };
  }

  private HasValue mockResult(String type, String name, long count) {
    var result = Mockito.mock(HasValue.class);
    var bean = Mockito.mock(BeanObject.class);
    Mockito.when(result.beanObject()).thenReturn(bean);
    Mockito.when(bean.getProperties()).thenReturn(Map.of("name", name, "type", type));
    Mockito.when(result.value()).thenReturn(count);
    return result;
  }
}
