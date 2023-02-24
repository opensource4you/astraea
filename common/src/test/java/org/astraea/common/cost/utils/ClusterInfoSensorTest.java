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

import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.collector.MetricCollector;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.it.Service;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ClusterInfoSensorTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(1).build();

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

      try (var mc = MetricCollector.builder().interval(Duration.ofSeconds(1)).build()) {
        mc.addMetricSensor(sensor);
        mc.registerLocalJmx(0);

        Utils.sleep(Duration.ofSeconds(2));

        var cb = mc.clusterBean();
        // assert contains that metrics
        mc.clusterBean()
            .all()
            .forEach(
                (broker, metrics) ->
                    Assertions.assertTrue(
                        metrics.stream().anyMatch(x -> x instanceof LogMetrics.Log.Gauge)));
        mc.clusterBean()
            .all()
            .forEach(
                (broker, metrics) ->
                    Assertions.assertTrue(
                        metrics.stream()
                            .anyMatch(x -> x instanceof ClusterInfoSensor.ReplicasCountMetric)));

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
        Assertions.assertTrue(
            info.replicaStream().allMatch(r -> r.nodeInfo().id() == aBroker.id()));
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
      }
    }
  }
}
