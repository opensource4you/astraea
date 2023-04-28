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
package org.astraea.common.cost;

import java.util.List;
import java.util.Map;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.JndiClient;
import org.astraea.common.metrics.MetricsTestUtils;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BrokerInputCostTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testCost() {
    // testBrokerCost
    var brokerInputCost = new BrokerInputCost();
    var clusterBean =
        ClusterBean.of(
            Map.of(1, List.of(meter(10000D)), 2, List.of(meter(20000D)), 3, List.of(meter(5000D))));
    var scores = brokerInputCost.brokerCost(ClusterInfo.empty(), clusterBean).value();
    Assertions.assertEquals(10000D, scores.get(1));
    Assertions.assertEquals(20000D, scores.get(2));
    Assertions.assertEquals(5000D, scores.get(3));

    // testClusterCost
    var clusterCost = brokerInputCost.clusterCost(ClusterInfo.empty(), clusterBean).value();
    Assertions.assertEquals(0.535, Math.round(clusterCost * 1000.0) / 1000.0);
  }

  @Test
  void testSensor() {
    try (var producer = Producer.of(SERVICE.bootstrapServers())) {
      producer.send(Record.builder().topic("test").key(new byte[100]).build());
      producer.flush();
    }
    var f = new BrokerInputCost();
    var clusterBean =
        MetricsTestUtils.clusterBean(
            Map.of(0, JndiClient.of(SERVICE.jmxServiceURL())), f.metricSensor().get());

    Assertions.assertNotEquals(
        0, clusterBean.brokerMetrics(0, ServerMetrics.BrokerTopic.Meter.class).count());

    // Test the fetched object's type, and its metric name.
    Assertions.assertTrue(
        clusterBean
            .brokerMetrics(0, ServerMetrics.BrokerTopic.Meter.class)
            .allMatch(
                o ->
                    ServerMetrics.BrokerTopic.BYTES_IN_PER_SEC
                        .metricName()
                        .equals(o.beanObject().properties().get("name"))));

    // Test the fetched object's value.
    Assertions.assertTrue(
        clusterBean
            .brokerMetrics(0, ServerMetrics.BrokerTopic.Meter.class)
            .allMatch(result -> result.count() != 0));
  }

  private static ServerMetrics.BrokerTopic.Meter meter(double value) {
    return new ServerMetrics.BrokerTopic.Meter(
        new BeanObject(
            "object",
            Map.of("name", ServerMetrics.BrokerTopic.BYTES_IN_PER_SEC.metricName()),
            Map.of("FifteenMinuteRate", value)));
  }
}
