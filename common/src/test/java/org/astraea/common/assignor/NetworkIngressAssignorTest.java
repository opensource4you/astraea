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
package org.astraea.common.assignor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.common.DataRate;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfoBuilder;
import org.astraea.common.admin.Replica;
import org.astraea.common.cost.NetworkIngressCost;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NetworkIngressAssignorTest {
  @Test
  void testGreedyAssign() {
    var clusterInfo =
        ClusterInfoBuilder.builder()
            .addNode(Set.of(1, 2, 3))
            .addFolders(
                Map.of(
                    1,
                    Set.of("/folder0", "/folder1"),
                    2,
                    Set.of("/folder0", "/folder1"),
                    3,
                    Set.of("/folder0", "/folder1")))
            .addTopic(
                "a",
                9,
                (short) 1,
                replica -> {
                  var factor = getFactor(replica.partition());
                  return Replica.builder(replica)
                      .size((long) (factor * DataRate.MB.of(10).perSecond().byteRate()))
                      .build();
                })
            .addTopic(
                "b",
                9,
                (short) 1,
                replica -> {
                  var factor = getFactor(replica.partition());
                  return Replica.builder(replica)
                      .size((long) (factor * DataRate.MB.of(10).perSecond().byteRate()))
                      .build();
                })
            .addTopic(
                "c",
                9,
                (short) 1,
                replica -> {
                  var factor = getFactor(replica.partition());
                  return Replica.builder(replica)
                      .size((long) (factor * DataRate.MB.of(10).perSecond().byteRate()))
                      .build();
                })
            .build();
    var clusterBean =
        ClusterBean.of(
            Map.of(
                1,
                List.of(
                    bandwidth(
                        ServerMetrics.Topic.BYTES_IN_PER_SEC,
                        "a",
                        DataRate.MB.of(90).perSecond().byteRate()),
                    bandwidth(
                        ServerMetrics.Topic.BYTES_IN_PER_SEC,
                        "b",
                        DataRate.MB.of(90).perSecond().byteRate()),
                    bandwidth(
                        ServerMetrics.Topic.BYTES_IN_PER_SEC,
                        "c",
                        DataRate.MB.of(90).perSecond().byteRate())),
                2,
                List.of(
                    bandwidth(
                        ServerMetrics.Topic.BYTES_IN_PER_SEC,
                        "a",
                        DataRate.MB.of(90).perSecond().byteRate()),
                    bandwidth(
                        ServerMetrics.Topic.BYTES_IN_PER_SEC,
                        "b",
                        DataRate.MB.of(90).perSecond().byteRate()),
                    bandwidth(
                        ServerMetrics.Topic.BYTES_IN_PER_SEC,
                        "c",
                        DataRate.MB.of(90).perSecond().byteRate())),
                3,
                List.of(
                    bandwidth(
                        ServerMetrics.Topic.BYTES_IN_PER_SEC,
                        "a",
                        DataRate.MB.of(90).perSecond().byteRate()),
                    bandwidth(
                        ServerMetrics.Topic.BYTES_IN_PER_SEC,
                        "b",
                        DataRate.MB.of(90).perSecond().byteRate()),
                    bandwidth(
                        ServerMetrics.Topic.BYTES_IN_PER_SEC,
                        "c",
                        DataRate.MB.of(90).perSecond().byteRate()))));

    var networkCost = new NetworkIngressCost();
    var cost = networkCost.partitionCost(clusterInfo, clusterBean).value();

    var topics = Set.of("a", "b", "c");
    var tpCostPerBroker =
        clusterInfo
            .replicaStream()
            .filter(Replica::isLeader)
            .filter(Replica::isOnline)
            .filter(replica -> topics.contains(replica.topic()))
            .collect(Collectors.groupingBy(replica -> replica.nodeInfo().id()))
            .entrySet()
            .stream()
            .map(
                e ->
                    Map.entry(
                        e.getKey(),
                        e.getValue().stream()
                            .map(
                                replica ->
                                    Map.entry(
                                        replica.topicPartition(),
                                        cost.get(replica.topicPartition())))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var assignor = new NetworkIngressAssignor();
    var assignment = assignor.greedyAssign(tpCostPerBroker, Set.of("haha1", "haha2", "haha3"));
    var costPerConsumer =
        assignment.entrySet().stream()
            .map(
                e -> {
                  var tps = e.getValue();
                  var tpCost =
                      tpCostPerBroker.values().stream()
                          .flatMap(v -> v.entrySet().stream())
                          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                  return Map.entry(e.getKey(), tps.stream().mapToDouble(tpCost::get).sum());
                })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    var sortedCostPerConsumer = new LinkedHashMap<String, Double>();
    costPerConsumer.entrySet().stream()
        .sorted(Map.Entry.comparingByValue())
        .forEach(e -> sortedCostPerConsumer.put(e.getKey(), e.getValue()));
    sortedCostPerConsumer.forEach((ignore, c) -> Assertions.assertTrue(c > 0.7 && c < 1.3));
  }

  static ServerMetrics.Topic.Meter bandwidth(
      ServerMetrics.Topic metric, String topic, double fifteenRate) {
    var domainName = "kafka.server";
    var properties =
        Map.of("type", "BrokerTopicMetric", "topic", topic, "name", metric.metricName());
    var attributes = Map.<String, Object>of("FifteenMinuteRate", fifteenRate);
    return new ServerMetrics.Topic.Meter(new BeanObject(domainName, properties, attributes));
  }

  private int getFactor(int partition) {
    if (partition > 2 && partition < 6) return 3;
    else if (partition >= 6) return 5;
    return 1;
  }
}
