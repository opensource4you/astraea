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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.common.Utils;
import org.astraea.app.metrics.BeanObject;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.MBeanClient;
import org.astraea.app.metrics.producer.HasProducerNodeMetrics;
import org.astraea.app.metrics.producer.ProducerMetrics;
import org.astraea.app.producer.Producer;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class NodeLatencyCostTest extends RequireBrokerCluster {

  @Test
  void testNan() {
    var bean = Mockito.mock(HasProducerNodeMetrics.class);
    Mockito.when(bean.brokerId()).thenReturn(1);
    Mockito.when(bean.requestLatencyAvg()).thenReturn(Double.NaN);
    var clusterBean = ClusterBean.of(Map.of(-1, List.of(bean)));
    var function = new NodeLatencyCost();
    var result = function.brokerCost(Mockito.mock(ClusterInfo.class), clusterBean);
    Assertions.assertEquals(0, result.value().size());
  }

  @Test
  void testBrokerId() {
    var bean = Mockito.mock(HasProducerNodeMetrics.class);
    Mockito.when(bean.brokerId()).thenReturn(1);
    Mockito.when(bean.requestLatencyAvg()).thenReturn(10D);
    var clusterBean = ClusterBean.of(Map.of(-1, List.of(bean)));
    var function = new NodeLatencyCost();
    var result = function.brokerCost(Mockito.mock(ClusterInfo.class), clusterBean);
    Assertions.assertEquals(1, result.value().size());
    Assertions.assertEquals(10D, result.value().get(1));
  }

  @Test
  void testCost() {
    var brokerId = brokerIds().iterator().next();
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).create();
      Utils.sleep(Duration.ofSeconds(3));
      producer.sender().topic(Utils.randomString(10)).value(new byte[100]).run();
      producer.flush();

      var function = new NodeLatencyCost();

      Utils.waitFor(
          () ->
              function
                      .brokerCost(
                          Mockito.mock(ClusterInfo.class),
                          ClusterBean.of(
                              Map.of(
                                  -1,
                                  ProducerMetrics.nodes(MBeanClient.local()).stream()
                                      .map(b -> (HasBeanObject) b)
                                      .collect(Collectors.toUnmodifiableList()))))
                      .value()
                      .size()
                  >= 1);
    }
  }

  @Test
  void testFetcher() {
    var function = new NodeLatencyCost();
    var client = Mockito.mock(MBeanClient.class);
    Mockito.when(client.queryBeans(Mockito.any()))
        .thenReturn(
            List.of(
                new BeanObject("a", Map.of("node-id", "node-10"), Map.of()),
                new BeanObject("a", Map.of("node-id", "node-10"), Map.of()),
                new BeanObject("a", Map.of("node-id", "node-11"), Map.of())));
    var result = function.fetcher().get().fetch(client);
    Assertions.assertEquals(3, result.size());
  }
}
