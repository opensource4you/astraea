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
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.collector.BeanCollector;
import org.astraea.common.metrics.collector.Receiver;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BrokerInputCostTest extends RequireBrokerCluster {

  @Test
  void testCost() {
    var brokerInputCost = new BrokerInputCost();
    var scores =
        brokerInputCost
            .brokerCost(
                ClusterInfo.empty(),
                ClusterBean.of(
                    Map.of(
                        1,
                        List.of(meter(10000D)),
                        2,
                        List.of(meter(20000D)),
                        3,
                        List.of(meter(5000D)))))
            .value();
    Assertions.assertEquals(10000D, scores.get(1));
    Assertions.assertEquals(20000D, scores.get(2));
    Assertions.assertEquals(5000D, scores.get(3));
  }

  @Test
  void testFetcher() {
    try (Receiver receiver =
        BeanCollector.builder()
            .build()
            .register()
            .host(jmxServiceURL().getHost())
            .port(jmxServiceURL().getPort())
            .fetcher(new BrokerInputCost().fetcher().get())
            .build()) {
      Assertions.assertFalse(receiver.current().isEmpty());

      // Test the fetched object's type, and its metric name.
      Assertions.assertTrue(
          receiver.current().stream()
              .allMatch(
                  o ->
                      (o instanceof ServerMetrics.Topic.Meter)
                          && (ServerMetrics.Topic.BYTES_IN_PER_SEC
                              .metricName()
                              .equals(o.beanObject().properties().get("name")))));

      // Test the fetched object's value.
      Assertions.assertTrue(
          receiver.current().stream()
              .map(o -> (ServerMetrics.Topic.Meter) o)
              .allMatch(result -> result.count() == 0));
    }
  }

  private static ServerMetrics.Topic.Meter meter(double value) {
    return new ServerMetrics.Topic.Meter(
        new BeanObject(
            "object",
            Map.of("name", ServerMetrics.Topic.BYTES_IN_PER_SEC.metricName()),
            Map.of("OneMinuteRate", value)));
  }
}
