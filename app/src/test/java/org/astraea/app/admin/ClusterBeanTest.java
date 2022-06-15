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
    Assertions.assertEquals(0, clusterInfo.beans().all().size());
    Assertions.assertEquals(0, clusterInfo.beans().getBeanObjectByPartition(20).size());
    Assertions.assertEquals(0, clusterInfo.beans().getBeanObjectByPartition(0).size());
    Assertions.assertEquals(0, clusterInfo.beans().getBeanObjectByReplica().size());
  }

  @Test
  void testBeans() {
    var clusterInfo = Mockito.mock(ClusterInfo.class);
    BeanObject testBeanObjectWithPartition =
        new BeanObject(
            "",
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
    BeanObject testBeanObjectWithoutPartition =
        new BeanObject(
            "",
            Map.of(
                "name",
                KafkaMetrics.ReplicaManager.LeaderCount.metricName(),
                "type",
                "ReplicaManager"),
            Map.of("Value", 100));
    // broker1 get partition-related beanObjects.
    // broker2 get not partition-related beanObjects.
    Mockito.when(clusterInfo.beans())
        .thenReturn(
            ClusterBean.of(
                Map.of(
                    1,
                    List.of(HasValue.of(testBeanObjectWithPartition)),
                    2,
                    List.of(HasValue.of(testBeanObjectWithoutPartition)))));
    // test all
    Assertions.assertEquals(2, clusterInfo.beans().all().size());
    Assertions.assertEquals(1, clusterInfo.beans().all().get(1).size());
    Assertions.assertEquals(1, clusterInfo.beans().all().get(2).size());
    // test get beanObject by partition
    Assertions.assertEquals(0, clusterInfo.beans().getBeanObjectByPartition(20).size());
    Assertions.assertEquals(1, clusterInfo.beans().getBeanObjectByPartition(1).size());
    Assertions.assertEquals(0, clusterInfo.beans().getBeanObjectByPartition(2).size());
    // test get beanObject by replica
    Assertions.assertEquals(1, clusterInfo.beans().getBeanObjectByReplica().size());
  }
}
