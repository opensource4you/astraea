/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.cost;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.astraea.app.admin.BeansGetter;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.jmx.BeanObject;
import org.astraea.app.metrics.kafka.BrokerTopicMetricsResult;
import org.astraea.app.metrics.kafka.KafkaMetrics;
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
