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
package org.astraea.common.cost;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NetworkIngressCostTest {
  static Function<Replica, Replica> generateReplica =
      (replica) -> Replica.builder(replica).size(1).build();

  @Test
  void testIncompatibility() {
    var networkCost =
        new NetworkIngressCost(
            new Configuration(
                Map.of(
                    NetworkIngressCost.TRAFFIC_INTERVAL,
                    "1Byte",
                    NetworkIngressCost.NETWORK_COST_ESTIMATION_METHOD,
                    "BROKER_TOPIC_FIFTEEN_MINUTE_RATE")));
    var topics =
        IntStream.range(0, 10)
            .mapToObj(i -> Utils.randomString(6))
            .collect(Collectors.toUnmodifiableList());
    var clusterInfo =
        ClusterInfo.builder()
            .addNode(Set.of(1))
            .addFolders(Map.of(1, Set.of("/folder0", "/folder1")))
            .build();

    for (var topic : topics) {
      clusterInfo =
          ClusterInfo.builder(clusterInfo).addTopic(topic, 1, (short) 1, generateReplica).build();
    }
    var index = new AtomicInteger(1);
    var clusterBean =
        ClusterBean.of(
            Map.of(
                1,
                topics.stream()
                    .map(
                        topic ->
                            bandwidth(
                                ServerMetrics.Topic.BYTES_IN_PER_SEC,
                                topic,
                                index.getAndIncrement()))
                    .collect(Collectors.toUnmodifiableList())));
    var partitionCost = networkCost.partitionCost(clusterInfo, clusterBean);
    var incompatible = partitionCost.incompatibility();

    Assertions.assertEquals(10, incompatible.size());
    Assertions.assertEquals(8, incompatible.get(TopicPartition.of(topics.get(0), 0)).size());
  }

  @Test
  void testEmptyIncompatibility() {
    var networkCost =
        new NetworkIngressCost(
            new Configuration(
                Map.of(
                    NetworkIngressCost.TRAFFIC_INTERVAL,
                    "1Byte",
                    NetworkIngressCost.NETWORK_COST_ESTIMATION_METHOD,
                    "BROKER_TOPIC_FIFTEEN_MINUTE_RATE")));
    var topics =
        IntStream.range(0, 10)
            .mapToObj(i -> Utils.randomString(6))
            .collect(Collectors.toUnmodifiableList());
    var clusterInfo =
        ClusterInfo.builder()
            .addNode(Set.of(1))
            .addFolders(Map.of(1, Set.of("/folder0", "/folder1")))
            .build();

    for (var topic : topics) {
      clusterInfo =
          ClusterInfo.builder(clusterInfo).addTopic(topic, 1, (short) 1, generateReplica).build();
    }
    var clusterBean =
        ClusterBean.of(
            Map.of(
                1,
                topics.stream()
                    .map(topic -> bandwidth(ServerMetrics.Topic.BYTES_IN_PER_SEC, topic, 1.0))
                    .collect(Collectors.toUnmodifiableList())));
    var partitionCost = networkCost.partitionCost(clusterInfo, clusterBean);
    var incompatible = partitionCost.incompatibility();

    Assertions.assertEquals(0, incompatible.size());
  }

  static double standardDeviation(List<Double> tpTraffic) {
    var avg = tpTraffic.stream().mapToDouble(i -> i).average().getAsDouble();
    return Math.sqrt(
        tpTraffic.stream().mapToDouble(v -> Math.pow(v - avg, 2)).average().getAsDouble());
  }

  static ServerMetrics.Topic.Meter bandwidth(
      ServerMetrics.Topic metric, String topic, double fifteenRate) {
    var domainName = "kafka.server";
    var properties =
        Map.of("type", "BrokerTopicMetric", "topic", topic, "name", metric.metricName());
    var attributes = Map.<String, Object>of("FifteenMinuteRate", fifteenRate);
    return new ServerMetrics.Topic.Meter(new BeanObject(domainName, properties, attributes));
  }
}
