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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.cost.StatisticalBean;
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

  @Test
  void testStatistic() {
    var topicName = "testStatistic";
    var brokerBean = new fakeStatisticalBean(new BeanObject("", Map.of(), Map.of("Value", "10")));
    var tprBean1 =
        new fakeStatisticalBean(
            new BeanObject(
                "", Map.of("topic", topicName, "partition", "0"), Map.of("Value", "100")));
    var tprBean2 =
        new fakeStatisticalBean(
            new BeanObject(
                "", Map.of("topic", topicName, "partition", "1"), Map.of("Value", "120")));
    var clusterBean = ClusterBean.of(Map.of(0, List.of(brokerBean, tprBean1, tprBean2)));
    Assertions.assertEquals(1, clusterBean.statisticsByNode().size());
    Assertions.assertEquals(2, clusterBean.statisticsByReplica().size());
    Assertions.assertEquals(
        "10", clusterBean.statisticsByNode().get(0).get(0).beanObject().attributes().get("Value"));
    Assertions.assertEquals(
        "100",
        clusterBean
            .statisticsByReplica()
            .get(TopicPartitionReplica.of(topicName, 0, 0))
            .get(0)
            .beanObject()
            .attributes()
            .get("Value"));
    Assertions.assertEquals(
        "120",
        clusterBean
            .statisticsByReplica()
            .get(TopicPartitionReplica.of(topicName, 1, 0))
            .get(0)
            .beanObject()
            .attributes()
            .get("Value"));
  }

  private class fakeStatisticalBean implements StatisticalBean {
    BeanObject beanObject;

    fakeStatisticalBean(BeanObject beanObject) {
      this.beanObject = beanObject;
    }

    @Override
    public BeanObject beanObject() {
      return beanObject;
    }
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
                  return new MetricType1(fakeTime, bean);
                case 1:
                  return new MetricType2(fakeTime, bean);
                case 2:
                  return new MetricType3(fakeTime, bean);
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
      // select a window of metric from a broker in ClusterBean
      var windowQuery =
          clusterBean.query(
              ClusterBeanQuery.window(MetricType1.class, 1).metricSince(500).ascending());
      System.out.println("[Window]");
      System.out.println(windowQuery);
    }

    {
      // select a window of metric from a broker in ClusterBean
      var windowQuery =
          clusterBean.query(
              ClusterBeanQuery.window(MetricType1.class, 1)
                  .metricSince(Duration.ofSeconds(3))
                  .descending());
      System.out.println("[Window]");
      System.out.println(windowQuery);
    }

    {
      // select the latest metric from a broker in ClusterBean
      var latestMetric = clusterBean.query(ClusterBeanQuery.latest(MetricType2.class, 1));
      System.out.println("[Latest]");
      System.out.println(latestMetric);
    }
  }

  private static class MetricType1 implements HasBeanObject {
    private final long createTime;
    private final BeanObject beanObject;

    private MetricType1(long createTime, BeanObject beanObject) {
      this.createTime = createTime;
      this.beanObject = beanObject;
    }

    @Override
    public BeanObject beanObject() {
      return beanObject;
    }

    @Override
    public long createdTimestamp() {
      return createTime;
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName()
          + "{"
          + "createTime="
          + createTime
          + ", beanObject="
          + beanObject
          + '}';
    }
  }

  private static class MetricType2 extends MetricType1 {
    private MetricType2(long createTime, BeanObject beanObject) {
      super(createTime, beanObject);
    }
  }

  private static class MetricType3 extends MetricType2 {
    private MetricType3(long createTime, BeanObject beanObject) {
      super(createTime, beanObject);
    }
  }
}
