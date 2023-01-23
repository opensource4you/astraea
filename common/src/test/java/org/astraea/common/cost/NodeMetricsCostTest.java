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

import java.time.Duration;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.client.HasNodeMetrics;
import org.astraea.common.metrics.client.producer.ProducerMetrics;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NodeMetricsCostTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testAllBrokersHaveCost() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var producer = Producer.of(SERVICE.bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(3).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));
      producer.send(
          Record.builder()
              .topic(topic)
              .partition(0)
              .key(new byte[100])
              .value(new byte[100])
              .build());
      producer.flush();
      var function = new NodeLatencyCost();

      var cost =
          function.brokerCost(
              admin
                  .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
                  .toCompletableFuture()
                  .join(),
              ClusterBean.of(
                  ProducerMetrics.nodes(MBeanClient.local()).stream()
                      .collect(Collectors.groupingBy(HasNodeMetrics::brokerId))));
      Assertions.assertEquals(3, cost.value().size());
      // only 1 node has latency metrics, so all costs are equal
      Assertions.assertEquals(1, cost.value().values().stream().distinct().count());

      // produce to another node to make another "latency"
      producer.send(
          Record.builder()
              .topic(topic)
              .partition(1)
              .key(new byte[100])
              .value(new byte[100])
              .build());

      var cost2 =
          function.brokerCost(
              admin
                  .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
                  .toCompletableFuture()
                  .join(),
              ClusterBean.of(
                  ProducerMetrics.nodes(MBeanClient.local()).stream()
                      .collect(Collectors.groupingBy(HasNodeMetrics::brokerId))));
      Assertions.assertEquals(3, cost2.value().size());
      // only 2 node has latency metrics. The other cost is equal to "max cost"
      Assertions.assertEquals(2, cost2.value().values().stream().distinct().count());
    }
  }
}
