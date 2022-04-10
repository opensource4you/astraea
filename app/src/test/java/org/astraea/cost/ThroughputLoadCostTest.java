package org.astraea.cost;

import static org.mockito.ArgumentMatchers.any;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.kafka.BrokerTopicMetricsResult;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.astraea.partitioner.smoothPartitioner.SmoothWeightMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ThroughputLoadCostTest {
  @Test
  void testComputeLoad() {
    var smooth = mockSmoothWeightMetrics();
    var loadCostFunction = new ThroughputLoadCost(smooth);
    var allBeans = exampleClusterInfo().allBeans();
    loadCostFunction.updateLoad(exampleClusterInfo());
    var load = loadCostFunction.cost(exampleClusterInfo());

    Assertions.assertEquals(0.61, load.get(1));
    Assertions.assertEquals(0.53, load.get(2));
    Assertions.assertEquals(0.37, load.get(3));

    smooth.updateMetrics(allBeans);
    loadCostFunction.updateLoad(exampleClusterInfo());

    // count does not change so all broker get one more score
    Assertions.assertEquals(0.81, load.get(1));
    Assertions.assertEquals(0.77, load.get(2));
    Assertions.assertEquals(0.69, load.get(3));
  }

  private ClusterInfo exampleClusterInfo() {
    var BytesInPerSec1 = mockResult(KafkaMetrics.BrokerTopic.BytesInPerSec.metricName(), 50000L);
    var BytesInPerSec2 = mockResult(KafkaMetrics.BrokerTopic.BytesInPerSec.metricName(), 100000L);
    var BytesInPerSec3 = mockResult(KafkaMetrics.BrokerTopic.BytesInPerSec.metricName(), 200000L);

    Collection<HasBeanObject> broker1 = List.of(BytesInPerSec1);
    Collection<HasBeanObject> broker2 = List.of(BytesInPerSec2);
    Collection<HasBeanObject> broker3 = List.of(BytesInPerSec3);
    return new FakeClusterInfo() {
      @Override
      public Map<Integer, Collection<HasBeanObject>> allBeans() {
        return Map.of(0, broker1, 1, broker2, 2, broker3);
      }
    };
  }

  private BrokerTopicMetricsResult mockResult(String name, long count) {
    var result = Mockito.mock(BrokerTopicMetricsResult.class);

    var bean = Mockito.mock(BeanObject.class);

    Mockito.when(result.beanObject()).thenReturn(bean);

    Mockito.when(bean.domainName()).thenReturn(KafkaMetrics.BrokerTopic.BytesInPerSec.metricName());
    Mockito.when(bean.getProperties()).thenReturn(Map.of("name", name));
    Mockito.when(result.count()).thenReturn(count);
    return result;
  }

  private SmoothWeightMetrics mockSmoothWeightMetrics() {
    var smoothWeightMetrics = Mockito.mock(SmoothWeightMetrics.class);
    Mockito.when(smoothWeightMetrics.inputCount()).thenReturn(Map.of(0, 10.0, 1, 5.0, 2, 1.0));
    Mockito.when(smoothWeightMetrics.outputCount()).thenReturn(Map.of(0, 10.0, 1, 5.0, 2, 1.0));
    Mockito.when(smoothWeightMetrics.jvmUsage()).thenReturn(Map.of(0, 10.0, 1, 5.0, 2, 1.0));
    Mockito.when(smoothWeightMetrics.cpuUsage()).thenReturn(Map.of(0, 10.0, 1, 5.0, 2, 1.0));
    Mockito.doNothing().when(smoothWeightMetrics).updateMetrics(any());
    return smoothWeightMetrics;
  }
}
