package org.astraea.cost;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.astraea.admin.BeansGetter;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.kafka.BrokerTopicMetricsResult;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class LoadCostTest {
  @Test
  void testComputeLoad() {
    var loadCostFunction = new LoadCost();
    var allBeans = exampleClusterInfo().beans().broker();
    var load = loadCostFunction.computeLoad(allBeans);

    Assertions.assertEquals(2, load.get(1));
    Assertions.assertEquals(1, load.get(2));
    Assertions.assertEquals(1, load.get(3));

    load = loadCostFunction.computeLoad(allBeans);

    // count does not change so all broker get one more score
    Assertions.assertEquals(3, load.get(1));
    Assertions.assertEquals(2, load.get(2));
    Assertions.assertEquals(2, load.get(3));
  }

  private ClusterInfo exampleClusterInfo() {
    var BytesInPerSec1 = mockResult(KafkaMetrics.BrokerTopic.BytesInPerSec.metricName(), 50000L);
    var BytesInPerSec2 = mockResult(KafkaMetrics.BrokerTopic.BytesInPerSec.metricName(), 100000L);
    var BytesInPerSec3 = mockResult(KafkaMetrics.BrokerTopic.BytesInPerSec.metricName(), 200000L);
    var BytesOutPerSec1 = mockResult(KafkaMetrics.BrokerTopic.BytesOutPerSec.metricName(), 210L);
    var BytesOutPerSec2 = mockResult(KafkaMetrics.BrokerTopic.BytesOutPerSec.metricName(), 20L);
    var BytesOutPerSec3 = mockResult(KafkaMetrics.BrokerTopic.BytesOutPerSec.metricName(), 10L);

    Collection<HasBeanObject> broker1 = List.of(BytesInPerSec1, BytesOutPerSec1);
    Collection<HasBeanObject> broker2 = List.of(BytesInPerSec2, BytesOutPerSec2);
    Collection<HasBeanObject> broker3 = List.of(BytesInPerSec3, BytesOutPerSec3);
    return new FakeClusterInfo() {
      @Override
      public BeansGetter beans() {
        return BeansGetter.of(Map.of(1, broker1, 2, broker2, 3, broker3));
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
