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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.jmx.BeanObject;
import org.astraea.app.metrics.kafka.HasValue;
import org.astraea.app.metrics.kafka.KafkaMetrics;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ReplicaDiskInCostTest extends RequireBrokerCluster {

  @Test
  void testPartitionCost() {
    Map<Integer, Integer> brokerBandwidth = new HashMap<>();
    brokerBandwidth.put(1, 50);
    brokerBandwidth.put(2, 50);
    brokerBandwidth.put(3, 50);
    var loadCostFunction = new ReplicaDiskInCost(brokerBandwidth);
    var broker1ReplicaLoad = loadCostFunction.partitionCost(exampleClusterInfo()).value(1);
    var broker2ReplicaLoad = loadCostFunction.partitionCost(exampleClusterInfo()).value(2);
    var broker3ReplicaLoad = loadCostFunction.partitionCost(exampleClusterInfo()).value(3);
    // broker1
    Assertions.assertEquals(
        0.23841381072998047, broker1ReplicaLoad.get(new TopicPartition("test-1", 0)));
    Assertions.assertEquals(
        0.1907339096069336, broker1ReplicaLoad.get(new TopicPartition("test-2", 0)));
    // broker2
    Assertions.assertEquals(
        0.23841381072998047, broker2ReplicaLoad.get(new TopicPartition("test-1", 0)));
    Assertions.assertEquals(
        0.476834774017334, broker2ReplicaLoad.get(new TopicPartition("test-1", 1)));
    // broker3
    Assertions.assertEquals(
        0.476834774017334, broker3ReplicaLoad.get(new TopicPartition("test-1", 1)));
    Assertions.assertEquals(
        0.1907339096069336, broker3ReplicaLoad.get(new TopicPartition("test-2", 0)));
  }

  @Test
  void testBrokerCost() {
    Map<Integer, Integer> properties = new HashMap<>();
    properties.put(1, 50);
    properties.put(2, 50);
    properties.put(3, 50);
    var loadCostFunction = new ReplicaDiskInCost(properties);
    var brokerLoad = loadCostFunction.brokerCost(exampleClusterInfo()).value();
    Assertions.assertEquals(0.23841381072998047 + 0.1907339096069336, brokerLoad.get(1));
    Assertions.assertEquals(0.23841381072998047 + 0.476834774017334, brokerLoad.get(2));
    Assertions.assertEquals(0.476834774017334 + 0.1907339096069336, brokerLoad.get(3));
  }

  private ClusterInfo exampleClusterInfo() {
    var oldTP1_0 =
        fakeBeanObject(
            "Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-1", "0", 1000, 1000L);
    var newTP1_0 =
        fakeBeanObject(
            "Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-1", "0", 50000000, 5000L);
    var oldTP1_1 =
        fakeBeanObject(
            "Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-1", "1", 500, 1000L);
    var newTP1_1 =
        fakeBeanObject(
            "Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-1", "1", 100000000, 5000L);
    var oldTP2_0 =
        fakeBeanObject(
            "Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-2", "0", 200, 1000L);
    var newTP2_0 =
        fakeBeanObject(
            "Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-2", "0", 40000000, 5000L);
    /*
    test replica distribution :
        broker1 : test-1-0,test-2-0
        broker2 : test-1-0,test-1-1
        broker1 : test-1-1,test-2-0
     */
    Collection<HasBeanObject> broker1 = List.of(oldTP1_0, newTP1_0, oldTP2_0, newTP2_0);
    Collection<HasBeanObject> broker2 = List.of(oldTP1_0, newTP1_0, oldTP1_1, newTP1_1);
    Collection<HasBeanObject> broker3 = List.of(oldTP1_1, newTP1_1, oldTP2_0, newTP2_0);
    return new FakeClusterInfo() {
      @Override
      public List<NodeInfo> nodes() {
        return List.of(NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1), NodeInfo.of(3, "", -1));
      }

      @Override
      public Set<String> topics() {
        return Set.of("test-1", "test-2");
      }

      @Override
      public List<ReplicaInfo> replicas(String topic) {
        if (topic.equals("test-1"))
          return List.of(
              ReplicaInfo.of("test-1", 0, NodeInfo.of(1, "", -1), true, true, false),
              ReplicaInfo.of("test-1", 0, NodeInfo.of(2, "", -1), false, true, false),
              ReplicaInfo.of("test-1", 1, NodeInfo.of(2, "", -1), false, true, false),
              ReplicaInfo.of("test-1", 1, NodeInfo.of(3, "", -1), true, true, false));
        else
          return List.of(
              ReplicaInfo.of("test-2", 0, NodeInfo.of(1, "", -1), false, true, false),
              ReplicaInfo.of("test-2", 0, NodeInfo.of(3, "", -1), true, true, false));
      }

      @Override
      public ClusterBean clusterBean() {
        return ClusterBean.of(Map.of(1, broker1, 2, broker2, 3, broker3));
      }
    };
  }

  private HasValue fakeBeanObject(
      String type, String name, String topic, String partition, long size, long time) {
    BeanObject beanObject =
        new BeanObject(
            "",
            Map.of("name", name, "type", type, "topic", topic, "partition", partition),
            Map.of("Value", size),
            time);
    return HasValue.of(beanObject);
  }
}
