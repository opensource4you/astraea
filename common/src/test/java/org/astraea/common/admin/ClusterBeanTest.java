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
package org.astraea.common.admin;

import java.util.List;
import java.util.Map;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.SensorBuilder;
import org.astraea.common.metrics.broker.HasGauge;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.stats.Avg;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ClusterBeanTest {

  @Test
  void testBeans() {
    // BeanObject1 and BeanObject2 is same partition in different broker
    BeanObject testBeanObjectWithPartition1 =
        new BeanObject(
            "kafka.log",
            Map.of(
                "name",
                LogMetrics.Log.SIZE.metricName(),
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
                LogMetrics.Log.SIZE.metricName(),
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
                LogMetrics.Log.LOG_END_OFFSET.name(),
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
                ServerMetrics.ReplicaManager.LEADER_COUNT.metricName(),
                "type",
                "ReplicaManager"),
            Map.of("Value", 300));
    var clusterBean =
        ClusterBean.of(
            Map.of(
                1,
                List.of(HasGauge.ofLong(testBeanObjectWithPartition1)),
                2,
                List.of(
                    HasGauge.ofLong(testBeanObjectWithoutPartition),
                    HasGauge.ofLong(testBeanObjectWithPartition2),
                    HasGauge.ofLong(testBeanObjectWithPartition3))));
    // test all
    Assertions.assertEquals(2, clusterBean.all().size());
    Assertions.assertEquals(1, clusterBean.all().get(1).size());
    Assertions.assertEquals(3, clusterBean.all().get(2).size());

    // test get beanObject by replica
    Assertions.assertEquals(2, clusterBean.mapByReplica().size());
    Assertions.assertEquals(
        2, clusterBean.mapByReplica().get(TopicPartitionReplica.of("testBeans", 0, 2)).size());
  }

  @Test
  void testStatistic() {
    var topicName = "testStatistic";
    var clusterBean = clusterBean(topicName);
    var tprValue = clusterBean.statisticsByReplica(LogMetrics.Log.SIZE.metricName(), Avg.AVG_KEY);
    var brokerValue =
        clusterBean.statisticsByNode(
            ServerMetrics.BrokerTopic.BYTES_IN_PER_SEC.metricName(), Avg.AVG_KEY);
    Assertions.assertEquals(
        (100.0 + 1000) / 2, tprValue.get(TopicPartitionReplica.of(topicName, 0, 0)));
    Assertions.assertEquals(
        (100.0 + 200) / 2, tprValue.get(TopicPartitionReplica.of(topicName, 1, 1)));
    Assertions.assertEquals(
        (10.0 + 20) / 2, tprValue.get(TopicPartitionReplica.of(topicName, 2, 2)));
    Assertions.assertEquals((100.0 + 1000) / 2, brokerValue.get(0));
    Assertions.assertEquals((100.0 + 200) / 2, brokerValue.get(1));
    Assertions.assertEquals((10.0 + 20) / 2, brokerValue.get(2));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static ClusterBean clusterBean(String topicName) {
    var sensors =
        List.of(
            new SensorBuilder().addStat(Avg.AVG_KEY, Avg.of()).build(),
            new SensorBuilder().addStat(Avg.AVG_KEY, Avg.of()).build(),
            new SensorBuilder().addStat(Avg.AVG_KEY, Avg.of()).build());
    sensors.get(0).record(100.0);
    sensors.get(0).record(1000.0);
    sensors.get(1).record(100.0);
    sensors.get(1).record(200.0);
    sensors.get(2).record(10.0);
    sensors.get(2).record(20.0);
    return ClusterBean.of(
        Map.of(),
        Map.of(
            LogMetrics.Log.SIZE.metricName(),
            Map.of(
                TopicPartitionReplica.of(topicName, 0, 0), sensors.get(0),
                TopicPartitionReplica.of(topicName, 1, 1), sensors.get(1),
                TopicPartitionReplica.of(topicName, 2, 2), sensors.get(2)),
            ServerMetrics.BrokerTopic.BYTES_IN_PER_SEC.metricName(),
            Map.of(
                0, sensors.get(0),
                1, sensors.get(1),
                2, sensors.get(2))));
  }
}
