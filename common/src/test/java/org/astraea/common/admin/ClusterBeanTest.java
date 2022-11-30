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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.HasBeanObject;
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

  Stream<HasBeanObject> random(int broker) {
    return Stream.generate(
            () -> {
              final var domainName = "test";
              final var properties =
                  Map.of(
                      "type", "testing",
                      "broker", String.valueOf(broker),
                      "name", "whatever");
              final var attributes =
                  Map.<String, Object>of("value", ThreadLocalRandom.current().nextInt());
              return new BeanObject(domainName, properties, attributes);
            })
        .map(
            bean -> {
              final var fakeTime = ThreadLocalRandom.current().nextLong(0, 1000);
              switch (ThreadLocalRandom.current().nextInt(0, 3)) {
                case 0:
                  return metric1(fakeTime, bean);
                case 1:
                  return metric2(fakeTime, bean);
                case 2:
                  return metric3(fakeTime, bean);
                default:
                  throw new RuntimeException();
              }
            });
  }

  @Test
  void testClusterBeanQuery() {
    var clusterBean =
        ClusterBean.of(
            Map.of(
                1, random(1).limit(1000).collect(Collectors.toUnmodifiableList()),
                2, random(2).limit(1000).collect(Collectors.toUnmodifiableList()),
                3, random(3).limit(1000).collect(Collectors.toUnmodifiableList())));

    {
      var metrics =
          clusterBean
              .select(MetricType1.class, 1)
              .metricSince(500)
              .metricQuantities(50)
              .descending()
              .run();

      //noinspection ConstantConditions
      Assertions.assertTrue(metrics.stream().allMatch(m -> m instanceof MetricType1));
      Assertions.assertTrue(metrics.stream().allMatch(m -> m.createdTimestamp() >= 500));
      Assertions.assertTrue(
          metrics.stream().allMatch(m -> m.beanObject().properties().get("broker").equals("1")));
      Assertions.assertEquals(50, metrics.size());
      Assertions.assertEquals(
          metrics.stream()
              .sorted(Comparator.comparingLong(HasBeanObject::createdTimestamp).reversed())
              .collect(Collectors.toUnmodifiableList()),
          metrics);
    }
  }

  private interface MetricType1 extends HasBeanObject {}

  private interface MetricType2 extends HasBeanObject {}

  private interface MetricType3 extends HasBeanObject {}

  private static MetricType1 metric1(long createTime, BeanObject bean) {
    return new MetricType1() {
      @Override
      public BeanObject beanObject() {
        return bean;
      }

      @Override
      public long createdTimestamp() {
        return createTime;
      }
    };
  }

  private static MetricType2 metric2(long createTime, BeanObject bean) {
    return new MetricType2() {
      @Override
      public BeanObject beanObject() {
        return bean;
      }

      @Override
      public long createdTimestamp() {
        return createTime;
      }
    };
  }

  private static MetricType3 metric3(long createTime, BeanObject bean) {
    return new MetricType3() {
      @Override
      public BeanObject beanObject() {
        return bean;
      }

      @Override
      public long createdTimestamp() {
        return createTime;
      }
    };
  }
}
