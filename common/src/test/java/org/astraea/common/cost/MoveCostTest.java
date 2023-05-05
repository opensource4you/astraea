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
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.collector.BeanObjectClient;
import org.astraea.common.metrics.collector.MetricSensor;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MoveCostTest {
  private static final Service SERVICE = Service.builder().numberOfBrokers(1).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testSensor() {
    // create topic partition to get metrics
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin.creator().topic("testSensor").numberOfPartitions(2).run().toCompletableFuture().join();
    }
    var cost1 = new ReplicaLeaderCost();
    var cost2 = new FakeCf();
    var mergeCost = HasMoveCost.of(List.of(cost1, cost2));
    var metrics =
        mergeCost
            .metricSensor()
            .fetch(
                BeanObjectClient.local(SERVICE.dataFolders().keySet().iterator().next()),
                ClusterBean.EMPTY);
    Assertions.assertEquals(3, metrics.size());
    Assertions.assertTrue(
        metrics.stream()
            .anyMatch(
                x ->
                    x.beanObject()
                        .properties()
                        .get("name")
                        .equals(ServerMetrics.ReplicaManager.LEADER_COUNT.metricName())));
    Assertions.assertTrue(
        metrics.stream()
            .anyMatch(
                x ->
                    x.beanObject()
                        .properties()
                        .get("name")
                        .equals(
                            ServerMetrics.BrokerTopic.REPLICATION_BYTES_IN_PER_SEC.metricName())));
    Assertions.assertTrue(
        metrics.stream()
            .anyMatch(
                x ->
                    x.beanObject()
                        .properties()
                        .get("name")
                        .equals(ServerMetrics.BrokerTopic.BYTES_IN_PER_SEC.metricName())));
  }

  class FakeCf implements HasMoveCost {
    @Override
    public MetricSensor metricSensor() {
      return MetricSensor.of(
          List.of(
              (c, ignored) ->
                  List.of(ServerMetrics.BrokerTopic.REPLICATION_BYTES_IN_PER_SEC.fetch(c)),
              (c, ignored) -> List.of(ServerMetrics.BrokerTopic.BYTES_IN_PER_SEC.fetch(c))));
    }

    @Override
    public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
      return null;
    }
  }
}
