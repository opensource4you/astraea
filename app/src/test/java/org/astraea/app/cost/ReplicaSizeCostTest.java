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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.ReplicaInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.common.Utils;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.KafkaMetrics;
import org.astraea.app.metrics.broker.HasValue;
import org.astraea.app.metrics.collector.BeanCollector;
import org.astraea.app.metrics.jmx.BeanObject;
import org.astraea.app.producer.Producer;
import org.astraea.app.service.RequireSingleBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ReplicaSizeCostTest extends RequireSingleBrokerCluster {
  private static final HasValue SIZE_TP1_0 =
      fakeBeanObject(
          "Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-1", "0", 891289600);
  private static final HasValue SIZE_TP1_1 =
      fakeBeanObject(
          "Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-1", "1", 471859200);
  private static final HasValue SIZE_TP2_0 =
      fakeBeanObject("Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-2", "0", 0);
  private static final HasValue SIZE_TP2_1 =
      fakeBeanObject(
          "Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-2", "1", 367001600);
  private static final Collection<HasBeanObject> BROKER_1 =
      List.of(SIZE_TP1_0, SIZE_TP1_1, SIZE_TP2_1);
  private static final Collection<HasBeanObject> BROKER_2 = List.of(SIZE_TP1_1, SIZE_TP2_0);
  private static final Collection<HasBeanObject> BROKER_3 =
      List.of(SIZE_TP2_1, SIZE_TP1_0, SIZE_TP2_0);

  @Test
  void testGetMetrics() throws ExecutionException, InterruptedException {
    var brokerDiskSize = Map.of(1, 1000, 2, 1000, 3, 1000);
    var topicName = "testGetMetrics-1";
    try (var admin = Admin.of(bootstrapServers())) {
      var host = "localhost";
      admin.creator().topic(topicName).numberOfPartitions(1).numberOfReplicas((short) 1).create();
      // wait for topic creation
      Utils.sleep(Duration.ofSeconds(5));
      var producer = Producer.builder().bootstrapServers(bootstrapServers()).build();
      producer.sender().topic(topicName).key(new byte[10000]).run().toCompletableFuture().get();
      ReplicaSizeCost costFunction = new ReplicaSizeCost(brokerDiskSize);
      var beanObjects =
          BeanCollector.builder()
              .interval(Duration.ofSeconds(4))
              .build()
              .register()
              .host(host)
              .port(jmxServiceURL().getPort())
              .fetcher(costFunction.fetcher().get())
              .build()
              .current();
      var replicaSize =
          beanObjects.stream()
              .filter(x -> x instanceof HasValue)
              .filter(x -> x.beanObject().properties().get("name").equals("Size"))
              .filter(x -> x.beanObject().properties().get("type").equals("Log"))
              .sorted(Comparator.comparing(HasBeanObject::createdTimestamp).reversed())
              .map(x -> (HasValue) x)
              .limit(1)
              .map(e2 -> (int) e2.value())
              .collect(Collectors.toList());
      Assertions.assertTrue(replicaSize.get(0) >= 10000);
    }
  }

  @Test
  void partitionCost() {
    var brokerDiskSize = Map.of(1, 1000, 2, 1000, 3, 1000);
    var loadCostFunction = new ReplicaSizeCost(brokerDiskSize);
    var broker1ReplicaLoad = loadCostFunction.partitionCost(clusterInfo(), clusterBean()).value(1);
    var BROKER_2ReplicaLoad = loadCostFunction.partitionCost(clusterInfo(), clusterBean()).value(2);
    var BROKER_3ReplicaLoad = loadCostFunction.partitionCost(clusterInfo(), clusterBean()).value(3);
    // broker1
    Assertions.assertEquals(0.85, broker1ReplicaLoad.get(TopicPartition.of("test-1", 0)));
    Assertions.assertEquals(0.45, broker1ReplicaLoad.get(TopicPartition.of("test-1", 1)));
    Assertions.assertEquals(0.35, broker1ReplicaLoad.get(TopicPartition.of("test-2", 1)));
    // BROKER_2
    Assertions.assertEquals(0.45, BROKER_2ReplicaLoad.get(TopicPartition.of("test-1", 1)));
    Assertions.assertEquals(0, BROKER_2ReplicaLoad.get(TopicPartition.of("test-2", 0)));
    // BROKER_3
    Assertions.assertEquals(0.85, BROKER_3ReplicaLoad.get(TopicPartition.of("test-1", 0)));
    Assertions.assertEquals(0, BROKER_3ReplicaLoad.get(TopicPartition.of("test-2", 0)));
    Assertions.assertEquals(0.35, BROKER_3ReplicaLoad.get(TopicPartition.of("test-2", 1)));
  }

  @Test
  void brokerCost() {
    var brokerDiskSize = Map.of(1, 1000, 2, 1000, 3, 1000);
    var loadCostFunction = new ReplicaSizeCost(brokerDiskSize);
    var brokerReplicaLoad = loadCostFunction.brokerCost(clusterInfo(), clusterBean()).value();
    Assertions.assertEquals(brokerReplicaLoad.get(1), 0.85 + 0.45 + 0.35);
    Assertions.assertEquals(brokerReplicaLoad.get(2), 0.45);
    Assertions.assertEquals(brokerReplicaLoad.get(3), 0.85 + 0.35);
  }

  private static ClusterInfo clusterInfo() {
    ClusterInfo clusterInfo = Mockito.mock(ClusterInfo.class);
    Mockito.when(clusterInfo.nodes())
        .thenReturn(
            List.of(NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1), NodeInfo.of(3, "", -1)));
    Mockito.when(clusterInfo.topics()).thenReturn(Set.of("test-1", "test-2"));
    Mockito.when(clusterInfo.replicas(Mockito.anyString()))
        .thenAnswer(
            topic ->
                topic.getArgument(0).equals("test-1")
                    ? List.of(
                        ReplicaInfo.of("test-1", 0, NodeInfo.of(1, "", -1), true, true, false),
                        ReplicaInfo.of("test-1", 0, NodeInfo.of(3, "", -1), false, true, false),
                        ReplicaInfo.of("test-1", 1, NodeInfo.of(1, "", -1), false, true, false),
                        ReplicaInfo.of("test-1", 1, NodeInfo.of(2, "", -1), true, true, false))
                    : List.of(
                        ReplicaInfo.of("test-2", 0, NodeInfo.of(2, "", -1), false, true, false),
                        ReplicaInfo.of("test-2", 0, NodeInfo.of(3, "", -1), true, true, false),
                        ReplicaInfo.of("test-2", 1, NodeInfo.of(1, "", -1), false, true, false),
                        ReplicaInfo.of("test-2", 1, NodeInfo.of(3, "", -1), true, true, false)));
    return clusterInfo;
  }

  private static ClusterBean clusterBean() {
    return ClusterBean.of(Map.of(1, BROKER_1, 2, BROKER_2, 3, BROKER_3));
  }

  private static HasValue fakeBeanObject(
      String type, String name, String topic, String partition, long size) {
    BeanObject beanObject =
        new BeanObject(
            "kafka.log",
            Map.of("name", name, "type", type, "topic", topic, "partition", partition),
            Map.of("Value", size));
    return HasValue.of(beanObject);
  }
}
