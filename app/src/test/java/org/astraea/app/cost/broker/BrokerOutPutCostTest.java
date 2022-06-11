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
package org.astraea.app.cost.broker;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.cost.FakeClusterInfo;
import org.astraea.app.cost.Normalizer;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.jmx.BeanObject;
import org.astraea.app.metrics.kafka.BrokerTopicMetricsResult;
import org.astraea.app.metrics.kafka.KafkaMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class BrokerOutPutCostTest {
  @Test
  void testCost() throws InterruptedException {
    ClusterInfo clusterInfo = exampleClusterInfo(10000L, 20000L, 5000L);

    var brokerOutputCost = new BrokerOutputCost();
    var scores = brokerOutputCost.brokerCost(clusterInfo).normalize(Normalizer.TScore()).value();
    Assertions.assertEquals(0.47, scores.get(1));
    Assertions.assertEquals(0.63, scores.get(2));
    Assertions.assertEquals(0.39, scores.get(3));

    ClusterInfo clusterInfo2 = exampleClusterInfo(55555L, 25352L, 25000L);
    scores = brokerOutputCost.brokerCost(clusterInfo2).normalize(Normalizer.TScore()).value();
    Assertions.assertEquals(0.64, scores.get(1));
    Assertions.assertEquals(0.43, scores.get(2));
    Assertions.assertEquals(0.43, scores.get(3));
  }

  private ClusterInfo exampleClusterInfo(long out1, long out2, long out3) {
    var BytesInPerSec1 = mockResult(KafkaMetrics.BrokerTopic.BytesOutPerSec.metricName(), out1);
    var BytesInPerSec2 = mockResult(KafkaMetrics.BrokerTopic.BytesOutPerSec.metricName(), out2);
    var BytesInPerSec3 = mockResult(KafkaMetrics.BrokerTopic.BytesOutPerSec.metricName(), out3);

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
