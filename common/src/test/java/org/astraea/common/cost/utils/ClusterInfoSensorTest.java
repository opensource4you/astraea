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
package org.astraea.common.cost.utils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.cost.LogMetricsGaugeBuilder;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.JndiClient;
import org.astraea.common.metrics.MetricFactory;
import org.astraea.common.metrics.MetricsTestUtils;
import org.astraea.common.metrics.broker.ClusterMetrics;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ClusterInfoSensorTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(1).build();

  @AfterAll
  public static void close() {
    SERVICE.close();
  }

  @Test
  void testClusterInfoSensor() {
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      var aBroker = admin.brokers().toCompletableFuture().join().get(0);
      var sensor = new ClusterInfoSensor();
      var topic = Utils.randomString();
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(3)
          .numberOfReplicas((short) 1)
          .run()
          .toCompletableFuture()
          .join();
      try (var producer = Producer.of(SERVICE.bootstrapServers())) {
        producer
            .send(
                IntStream.range(0, 100)
                    .mapToObj(x -> Record.builder().topic(topic).value(new byte[x]).build())
                    .collect(Collectors.toUnmodifiableList()))
            .forEach(i -> i.toCompletableFuture().join());
      }

      var cb = MetricsTestUtils.clusterBean(Map.of(aBroker.id(), JndiClient.local()), sensor);

      // assert contains that metrics
      cb.all()
          .forEach(
              (broker, metrics) ->
                  Assertions.assertTrue(
                      metrics.stream().anyMatch(x -> x instanceof LogMetrics.Log.Gauge)));
      cb.all()
          .forEach(
              (broker, metrics) ->
                  Assertions.assertTrue(
                      metrics.stream().anyMatch(x -> x instanceof ClusterMetrics.PartitionMetric)));

      var info = ClusterInfoSensor.metricViewCluster(cb);
      // compare topic
      Assertions.assertTrue(info.topicNames().contains(topic));
      // compare topic partition
      Assertions.assertTrue(
          info.topicPartitions()
              .containsAll(
                  Set.of(
                      TopicPartition.of(topic, 0),
                      TopicPartition.of(topic, 1),
                      TopicPartition.of(topic, 2))));
      // compare broker id
      Assertions.assertTrue(info.replicaStream().allMatch(r -> r.brokerId() == aBroker.id()));
      // compare replica size
      var realCluster = admin.clusterInfo(Set.of(topic)).toCompletableFuture().join();
      Assertions.assertTrue(
          info.replicaStream()
              .allMatch(
                  metricReplica -> {
                    var realReplica =
                        realCluster
                            .replicaStream()
                            .filter(
                                x ->
                                    x.topicPartitionReplica()
                                        .equals(metricReplica.topicPartitionReplica()))
                            .findFirst()
                            .orElseThrow();
                    return realReplica.size() == metricReplica.size();
                  }));
      // compare cluster id
      Assertions.assertEquals(realCluster.clusterId(), info.clusterId());
    }
  }

  @Test
  void testFollower() {
    var cb =
        ClusterBean.of(
            Map.ofEntries(
                Map.entry(
                    1,
                    List.of(
                        MetricFactory.ofPartitionMetric("TwoReplica", 0, 2),
                        new LogMetricsGaugeBuilder(LogMetrics.Log.SIZE)
                            .topic("TwoReplica")
                            .partition(0)
                            .logSize(200)
                            .build(),
                        MetricFactory.ofPartitionMetric("OneReplica", 0, 1),
                        new LogMetricsGaugeBuilder(LogMetrics.Log.SIZE)
                            .topic("OneReplica")
                            .partition(0)
                            .logSize(100)
                            .build())),
                Map.entry(
                    2,
                    List.of(
                        MetricFactory.ofPartitionMetric("TwoReplica", 0, 0),
                        new LogMetricsGaugeBuilder(LogMetrics.Log.SIZE)
                            .topic("TwoReplica")
                            .partition(0)
                            .logSize(150)
                            .build()))));
    var info = ClusterInfoSensor.metricViewCluster(cb);

    Assertions.assertEquals(Set.of("TwoReplica", "OneReplica"), info.topicNames());
    Assertions.assertEquals(
        Set.of(TopicPartition.of("TwoReplica", 0), TopicPartition.of("OneReplica", 0)),
        info.topicPartitions());
    Assertions.assertEquals(1, info.replicas("OneReplica").size());
    Assertions.assertEquals(100, info.replicas("OneReplica").get(0).size());
    Assertions.assertEquals(2, info.replicas("TwoReplica").size());
    Assertions.assertEquals(1, info.replicaLeaders("TwoReplica").size());
    Assertions.assertEquals(200, info.replicaLeaders("TwoReplica").get(0).size());
    Assertions.assertEquals(
        150, info.replicaStream().filter(Replica::isFollower).findFirst().orElseThrow().size());
  }

  @Test
  void testVariousReplicaFactor() {
    var topic =
        new Object() {
          Stream<HasBeanObject> partition(int partition, int replica) {
            return Stream.of(
                MetricFactory.ofPartitionMetric("topic", partition, replica),
                new LogMetricsGaugeBuilder(LogMetrics.Log.SIZE)
                    .topic("topic")
                    .partition(partition)
                    .logSize(0)
                    .build());
          }
        };

    var cb =
        ClusterBean.of(
            Map.ofEntries(
                Map.entry(
                    1,
                    Stream.of(topic.partition(0, 1), topic.partition(1, 2), topic.partition(2, 3))
                        .flatMap(x -> x)
                        .collect(Collectors.toUnmodifiableList())),
                Map.entry(
                    2,
                    Stream.of(topic.partition(1, 0), topic.partition(2, 0))
                        .flatMap(x -> x)
                        .collect(Collectors.toUnmodifiableList())),
                Map.entry(
                    3,
                    Stream.of(topic.partition(2, 0))
                        .flatMap(x -> x)
                        .collect(Collectors.toUnmodifiableList()))));
    var info = ClusterInfoSensor.metricViewCluster(cb);

    Assertions.assertEquals(Set.of("topic"), info.topicNames());
    Assertions.assertEquals(
        Set.of(
            TopicPartition.of("topic", 0),
            TopicPartition.of("topic", 1),
            TopicPartition.of("topic", 2)),
        info.topicPartitions());
    Assertions.assertEquals(3, info.replicaStream(1).count());
    Assertions.assertEquals(2, info.replicaStream(2).count());
    Assertions.assertEquals(1, info.replicaStream(3).count());
    Assertions.assertEquals(1, info.replicas(TopicPartition.of("topic", 0)).size());
    Assertions.assertEquals(2, info.replicas(TopicPartition.of("topic", 1)).size());
    Assertions.assertEquals(3, info.replicas(TopicPartition.of("topic", 2)).size());
    Assertions.assertTrue(info.replicaStream(1).allMatch(Replica::isLeader));
    Assertions.assertTrue(info.replicaStream(2).allMatch(Replica::isFollower));
    Assertions.assertTrue(info.replicaStream(3).allMatch(Replica::isFollower));
  }

  @Test
  void testClusterId() {
    var id = Utils.randomString();
    var cb =
        ClusterBean.of(
            Map.of(
                0,
                List.of(
                    new ServerMetrics.KafkaServer.ClusterIdGauge(
                        new BeanObject(
                            ServerMetrics.DOMAIN_NAME,
                            Map.of(
                                "type",
                                "KafkaServer",
                                "name",
                                ServerMetrics.KafkaServer.CLUSTER_ID.metricName()),
                            Map.of("Value", id))))));

    var info = ClusterInfoSensor.metricViewCluster(cb);

    Assertions.assertEquals(1, info.brokers().size());
    Assertions.assertEquals(id, info.clusterId());
  }

  @Test
  void testPartialMetricsException() {
    // The ClusterBean states there is a topic-0, but no relevant log size metrics found.
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            ClusterInfoSensor.metricViewCluster(
                ClusterBean.of(
                    Map.of(1, List.of(MetricFactory.ofPartitionMetric("topic", 0, 0))))));
  }
}
