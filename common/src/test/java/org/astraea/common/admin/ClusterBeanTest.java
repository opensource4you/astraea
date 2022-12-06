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
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.Utils;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.broker.HasGauge;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.broker.ServerMetrics;
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

  static List<String> fakeTopics =
      IntStream.range(0, 100)
          .mapToObj(i -> Utils.randomString())
          .collect(Collectors.toUnmodifiableList());

  Stream<ServerMetrics.Topic.Meter> random() {
    return IntStream.iterate(0, n -> n + 1)
        .mapToObj(
            index -> {
              var domainName = "kafka.server";
              var properties =
                  Map.of(
                      "type", "BrokerTopicMetrics",
                      "topic", fakeTopics.get(index % 100),
                      "name", "BytesInPerSec");
              var attributes =
                  Map.<String, Object>of("count", ThreadLocalRandom.current().nextInt());
              return new ServerMetrics.Topic.Meter(
                  new BeanObject(domainName, properties, attributes));
            });
  }

  @Test
  void testMapping() {
    ClusterBean clusterBean =
        ClusterBean.of(
            Map.of(
                1, random().limit(300).collect(Collectors.toUnmodifiableList()),
                2, random().limit(300).collect(Collectors.toUnmodifiableList()),
                3, random().limit(300).collect(Collectors.toUnmodifiableList())));

    Map<BrokerTopic, List<ServerMetrics.Topic.Meter>> result =
        clusterBean.mapByBrokerTopic(ServerMetrics.Topic.Meter.class);
    result.forEach(
        (key, metrics) -> {
          Assertions.assertEquals(3 * 100, result.size());
          Assertions.assertInstanceOf(BrokerTopic.class, key);
          metrics.forEach(
              metric -> Assertions.assertInstanceOf(ServerMetrics.Topic.Meter.class, metric));
        });
  }
}
