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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.Utils;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.broker.HasGauge;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.platform.JvmMemory;
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
      IntStream.range(0, 10)
          .mapToObj(i -> Utils.randomString())
          .collect(Collectors.toUnmodifiableList());

  Stream<? extends HasBeanObject> random(int seed) {
    var random = new Random(seed);
    return Stream.generate(
        () -> {
          switch (random.nextInt(5)) {
            case 0: // topic metrics
            case 1: // broker topic metrics
              {
                var domainName = ServerMetrics.DOMAIN_NAME;
                var properties =
                    Map.of(
                        "type", "BrokerTopicMetrics",
                        "topic", fakeTopics.get(random.nextInt(10)),
                        "name", "BytesInPerSec");
                return new ServerMetrics.Topic.Meter(
                    new BeanObject(domainName, properties, Map.of()));
              }
            case 2: // topic/partition metrics
            case 3: // topic/partition/replica metrics
              {
                var domainName = LogMetrics.DOMAIN_NAME;
                var properties =
                    Map.of(
                        "type",
                        "Log",
                        "topic",
                        fakeTopics.get(random.nextInt(10)),
                        "partition",
                        String.valueOf(random.nextInt(3)),
                        "name",
                        "Size");
                return new LogMetrics.Log.Gauge(new BeanObject(domainName, properties, Map.of()));
              }
            case 4: // noise
              {
                var domainName = "RandomMetrics";
                var properties = new HashMap<String, String>();
                properties.put("name", "whatever-" + (random.nextInt(100)));
                properties.put("type", "something");
                if (random.nextInt(2) == 0)
                  properties.put("topic", fakeTopics.get(random.nextInt(3)));
                if (random.nextInt(2) == 0)
                  properties.put("partition", String.valueOf(random.nextInt(3)));
                var beanObject = new BeanObject(domainName, properties, Map.of());
                return () -> beanObject;
              }
            default:
              throw new RuntimeException();
          }
        });
  }

  ClusterBean cb =
      ClusterBean.of(
          Map.of(
              1, random(0x0ae10).limit(3000).collect(Collectors.toUnmodifiableList()),
              2, random(0x0f0c1).limit(3000).collect(Collectors.toUnmodifiableList()),
              3, random(0x4040f).limit(3000).collect(Collectors.toUnmodifiableList())));
  Set<? extends HasBeanObject> allMetrics =
      cb.all().values().stream()
          .flatMap(Collection::stream)
          .collect(Collectors.toUnmodifiableSet());

  @Test
  void testMetricQuery() {
    // test index lookup
    var allTopicIndex =
        allMetrics.stream()
            .map(HasBeanObject::topicIndex)
            .flatMap(Optional::stream)
            .collect(Collectors.toUnmodifiableSet());
    Assertions.assertEquals(allTopicIndex, cb.topics());

    var allPartitionIndex =
        allMetrics.stream()
            .map(HasBeanObject::partitionIndex)
            .flatMap(Optional::stream)
            .collect(Collectors.toUnmodifiableSet());
    Assertions.assertEquals(allPartitionIndex, cb.partitions());

    var allReplicaIndex =
        cb.all().entrySet().stream()
            .flatMap(e -> e.getValue().stream().map(ee -> ee.replicaIndex(e.getKey())))
            .flatMap(Optional::stream)
            .collect(Collectors.toUnmodifiableSet());
    Assertions.assertEquals(allReplicaIndex, cb.replicas());

    var allBrokerTopicIndex =
        cb.all().entrySet().stream()
            .flatMap(e -> e.getValue().stream().map(ee -> ee.brokerTopicIndex(e.getKey())))
            .flatMap(Optional::stream)
            .collect(Collectors.toUnmodifiableSet());
    Assertions.assertEquals(allBrokerTopicIndex, cb.brokerTopics());

    // test query
    var allTopicMetrics =
        allMetrics.stream()
            .filter(bean -> bean.topicIndex().isPresent())
            .filter(bean -> bean instanceof ServerMetrics.Topic.Meter)
            .collect(
                Collectors.groupingBy(
                    bean -> bean.topicIndex().orElseThrow(), Collectors.toUnmodifiableSet()));
    Assertions.assertEquals(
        allTopicMetrics,
        fakeTopics.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    t -> t,
                    t ->
                        cb.topicMetrics(t, ServerMetrics.Topic.Meter.class)
                            .collect(Collectors.toUnmodifiableSet()))));

    var allPartitionMetrics =
        allMetrics.stream()
            .filter(bean -> bean.partitionIndex().isPresent())
            .filter(bean -> bean instanceof LogMetrics.Log.Gauge)
            .collect(
                Collectors.groupingBy(
                    bean -> bean.partitionIndex().orElseThrow(), Collectors.toUnmodifiableSet()));
    Assertions.assertEquals(
        allPartitionMetrics,
        fakeTopics.stream()
            .flatMap(t -> IntStream.range(0, 3).mapToObj(p -> TopicPartition.of(t, p)))
            .collect(
                Collectors.toUnmodifiableMap(
                    tp -> tp,
                    tp ->
                        cb.partitionMetrics(tp, LogMetrics.Log.Gauge.class)
                            .collect(Collectors.toUnmodifiableSet()))));

    var allReplicaMetrics =
        cb.all().entrySet().stream()
            .flatMap(
                e ->
                    e.getValue().stream()
                        .filter(ee -> ee.replicaIndex(e.getKey()).isPresent())
                        .filter(ee -> ee instanceof LogMetrics.Log.Gauge)
                        .map(ee -> Map.entry(ee.replicaIndex(e.getKey()).orElseThrow(), ee)))
            .collect(
                Collectors.groupingBy(
                    Map.Entry::getKey,
                    Collectors.mapping(Map.Entry::getValue, Collectors.toUnmodifiableSet())));
    Assertions.assertEquals(
        allReplicaMetrics,
        fakeTopics.stream()
            .flatMap(
                t ->
                    IntStream.range(0, 3)
                        .mapToObj(p -> TopicPartition.of(t, p))
                        .flatMap(
                            p ->
                                IntStream.rangeClosed(1, 3)
                                    .mapToObj(
                                        b ->
                                            TopicPartitionReplica.of(p.topic(), p.partition(), b))))
            .collect(
                Collectors.toUnmodifiableMap(
                    tpr -> tpr,
                    tpr ->
                        cb.replicaMetrics(tpr, LogMetrics.Log.Gauge.class)
                            .collect(Collectors.toUnmodifiableSet()))));

    var allBrokerTopicMetrics =
        cb.all().entrySet().stream()
            .flatMap(
                e ->
                    e.getValue().stream()
                        .filter(ee -> ee.brokerTopicIndex(e.getKey()).isPresent())
                        .filter(ee -> ee instanceof ServerMetrics.Topic.Meter)
                        .map(ee -> Map.entry(ee.brokerTopicIndex(e.getKey()).orElseThrow(), ee)))
            .collect(
                Collectors.groupingBy(
                    Map.Entry::getKey,
                    Collectors.mapping(Map.Entry::getValue, Collectors.toUnmodifiableSet())));
    Assertions.assertEquals(
        allBrokerTopicMetrics,
        fakeTopics.stream()
            .flatMap(t -> IntStream.rangeClosed(1, 3).mapToObj(b -> BrokerTopic.of(b, t)))
            .collect(
                Collectors.toUnmodifiableMap(
                    bt -> bt,
                    bt ->
                        cb.brokerTopicMetrics(bt, ServerMetrics.Topic.Meter.class)
                            .collect(Collectors.toUnmodifiableSet()))));

    // test empty query
    Assertions.assertEquals(
        Set.of(), cb.topicMetrics(fakeTopics.get(0), JvmMemory.class).collect(Collectors.toSet()));
    Assertions.assertEquals(
        Set.of(),
        cb.partitionMetrics(TopicPartition.of(fakeTopics.get(0), 0), JvmMemory.class)
            .collect(Collectors.toSet()));
    Assertions.assertEquals(
        Set.of(),
        cb.replicaMetrics(TopicPartitionReplica.of(fakeTopics.get(0), 0, 1), JvmMemory.class)
            .collect(Collectors.toSet()));
    Assertions.assertEquals(
        Set.of(),
        cb.brokerTopicMetrics(BrokerTopic.of(1, fakeTopics.get(0)), JvmMemory.class)
            .collect(Collectors.toSet()));
  }
}
