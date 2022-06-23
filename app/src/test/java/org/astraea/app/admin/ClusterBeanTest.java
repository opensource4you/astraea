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
package org.astraea.app.admin;

import java.util.List;
import java.util.Map;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.metrics.jmx.BeanObject;
import org.astraea.app.metrics.kafka.HasValue;
import org.astraea.app.metrics.kafka.KafkaMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ClusterBeanTest {

  @Test
  void testEmptyBeans() {
    var clusterInfo = ClusterInfo.of(Mockito.mock(org.apache.kafka.common.Cluster.class));
    Assertions.assertEquals(0, clusterInfo.clusterBean().all().size());
    Assertions.assertEquals(0, clusterInfo.clusterBean().beanObjectByPartition().size());
    Assertions.assertEquals(0, clusterInfo.clusterBean().beanObjectByReplica().size());
  }

  @Test
  void testBeans() {
    var clusterInfo = Mockito.mock(ClusterInfo.class);
    // BeanObject1 and BeanObject2 is same partition in different broker
    BeanObject testBeanObjectWithPartition1 =
        new BeanObject(
            "kafka.log",
            Map.of(
                "name",
                KafkaMetrics.TopicPartition.Size.metricName(),
                "type",
                "Log",
                "topic",
                "testBeans",
                "partition",
                "0"),
            Map.of("Value", 100));
    BeanObject testBeanObjectWithPartition2 =
        new BeanObject(
            "kafka.log",
            Map.of(
                "name",
                KafkaMetrics.TopicPartition.Size.metricName(),
                "type",
                "Log",
                "topic",
                "testBeans",
                "partition",
                "0"),
            Map.of("Value", 100));
    BeanObject testBeanObjectWithPartition3 =
        new BeanObject(
            "kafka.log",
            Map.of(
                "name",
                KafkaMetrics.TopicPartition.LogEndOffset.name(),
                "type",
                "Log",
                "topic",
                "testBeans",
                "partition",
                "0"),
            Map.of("Value", 100));
    BeanObject testBeanObjectWithoutPartition =
        new BeanObject(
            "kafka.log",
            Map.of(
                "name",
                KafkaMetrics.ReplicaManager.LeaderCount.metricName(),
                "type",
                "ReplicaManager"),
            Map.of("Value", 300));
    Mockito.when(clusterInfo.clusterBean())
        .thenReturn(
            ClusterBean.of(
                Map.of(
                    1,
                    List.of(HasValue.of(testBeanObjectWithPartition1)),
                    2,
                    List.of(
                        HasValue.of(testBeanObjectWithoutPartition),
                        HasValue.of(testBeanObjectWithPartition2),
                        HasValue.of(testBeanObjectWithPartition3)))));
    // test all
    clusterInfo.clusterBean().beanObjectByPartition();
    Assertions.assertEquals(2, clusterInfo.clusterBean().all().size());
    Assertions.assertEquals(1, clusterInfo.clusterBean().all().get(1).size());
    Assertions.assertEquals(3, clusterInfo.clusterBean().all().get(2).size());

    // test get beanObject by partition
    // when call beanObjectByPartition() will return a map and the key is TopicPartition it's will
    // ignore replicas and get the metrics of first replicas
    Assertions.assertEquals(
        2,
        clusterInfo
            .clusterBean()
            .beanObjectByPartition()
            .get(TopicPartition.of("testBeans", "0"))
            .size());
    // test get beanObject by replica
    Assertions.assertEquals(2, clusterInfo.clusterBean().beanObjectByReplica().size());
    Assertions.assertEquals(
        2,
        clusterInfo
            .clusterBean()
            .beanObjectByReplica()
            .get(TopicPartitionReplica.of("testBeans", "0", 2))
            .size());
  }
}
