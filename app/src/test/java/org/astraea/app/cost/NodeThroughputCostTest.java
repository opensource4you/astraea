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

import java.util.List;
import java.util.Map;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.metrics.BeanObject;
import org.astraea.app.metrics.MBeanClient;
import org.astraea.app.metrics.producer.HasProducerNodeMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class NodeThroughputCostTest {

  @Test
  void testNan() {
    var bean = Mockito.mock(HasProducerNodeMetrics.class);
    Mockito.when(bean.brokerId()).thenReturn(1);
    Mockito.when(bean.incomingByteRate()).thenReturn(Double.NaN);
    Mockito.when(bean.outgoingByteRate()).thenReturn(Double.NaN);
    var clusterBean = ClusterBean.of(Map.of(-1, List.of(bean)));
    var function = new NodeThroughputCost();
    var result = function.brokerCost(Mockito.mock(ClusterInfo.class), clusterBean);
    Assertions.assertEquals(0, result.value().size());
  }

  @Test
  void testBrokerId() {
    var bean = Mockito.mock(HasProducerNodeMetrics.class);
    Mockito.when(bean.brokerId()).thenReturn(1);
    Mockito.when(bean.incomingByteRate()).thenReturn(10D);
    Mockito.when(bean.outgoingByteRate()).thenReturn(10D);
    var clusterBean = ClusterBean.of(Map.of(-1, List.of(bean)));
    var function = new NodeThroughputCost();
    var result = function.brokerCost(Mockito.mock(ClusterInfo.class), clusterBean);
    Assertions.assertEquals(1, result.value().size());
    Assertions.assertEquals(20D, result.value().get(1));
  }

  @Test
  void testCost() {
    var throughputCost = new NodeThroughputCost();
    var bean0 = Mockito.mock(HasProducerNodeMetrics.class);
    Mockito.when(bean0.incomingByteRate()).thenReturn(10D);
    Mockito.when(bean0.outgoingByteRate()).thenReturn(20D);
    Mockito.when(bean0.brokerId()).thenReturn(10);
    var bean1 = Mockito.mock(HasProducerNodeMetrics.class);
    Mockito.when(bean1.incomingByteRate()).thenReturn(2D);
    Mockito.when(bean1.outgoingByteRate()).thenReturn(3D);
    Mockito.when(bean1.brokerId()).thenReturn(11);
    var clusterBean = ClusterBean.of(Map.of(0, List.of(bean0), 1, List.of(bean1)));
    var clusterInfo = Mockito.mock(ClusterInfo.class);
    var cost = throughputCost.brokerCost(clusterInfo, clusterBean);
    Assertions.assertEquals(30D, cost.value().get(10));
    Assertions.assertEquals(5D, cost.value().get(11));
  }

  @Test
  void testFetcher() {
    var throughputCost = new NodeThroughputCost();
    var fetcher = throughputCost.fetcher().get();
    var bean = new BeanObject("aaa", Map.of("node-id", "node-1"), Map.of());
    var client = Mockito.mock(MBeanClient.class);
    Mockito.when(client.queryBeans(Mockito.any())).thenReturn(List.of(bean));
    var result = fetcher.fetch(client);
    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals(bean, result.iterator().next().beanObject());
  }
}
