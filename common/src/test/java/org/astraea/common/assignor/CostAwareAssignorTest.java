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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import org.astraea.common.DataRate;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ClusterInfoBuilder;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.cost.NetworkIngressCost;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CostAwareAssignorTest {

  @Test
  void testEstimateIntervalTraffic() {
    var assignor = new CostAwareAssignor();
    var cost = new NetworkIngressCost();
    var aFactorList = new ArrayList<Double>();
    IntStream.range(0, 3)
        .forEach(
            i -> {
              var rand = new Random();
              aFactorList.add(i, rand.nextDouble());
            });
    var clusterInfo =
        ClusterInfoBuilder.builder()
            .addNode(Set.of(1))
            .addFolders(Map.of(1, Set.of("/folder0", "/folder1")))
            .addTopic(
                "a",
                3,
                (short) 1,
                replica -> {
                  var factor = aFactorList.get(replica.partition());
                  return Replica.builder(replica)
                      .size((long) (factor * DataRate.MiB.of(100).perSecond().byteRate()))
                      .build();
                })
            .addTopic(
                "b",
                1,
                (short) 1,
                replica ->
                    Replica.builder(replica)
                        .size((long) DataRate.MiB.of(10).perSecond().byteRate())
                        .build())
            .build();
    var clusterBean =
        ClusterBean.of(
            Map.of(
                1,
                List.of(
                    bandwidth(
                        ServerMetrics.Topic.BYTES_IN_PER_SEC,
                        "a",
                        DataRate.MiB.of(100).perSecond().byteRate()),
                    bandwidth(
                        ServerMetrics.Topic.BYTES_IN_PER_SEC,
                        "b",
                        DataRate.MiB.of(10).perSecond().byteRate()))));
    var costPerBroker =
        assignor.wrapCostBaseOnNode(
            clusterInfo, Set.of("a", "b"), cost.partitionCost(clusterInfo, clusterBean).value());
    var resultOf10MiBCost =
        assignor.estimateIntervalTraffic(clusterInfo, clusterBean, costPerBroker);
    var _10MiBCost = costPerBroker.get(1).get(TopicPartition.of("b-0"));
    var format = new DecimalFormat("#.####");
    //    Assertions.assertEquals(_10MiBCost, resultOf10MiBCost.get(1));
    Assertions.assertEquals(
        Double.parseDouble(format.format(_10MiBCost)),
        Double.parseDouble(format.format(resultOf10MiBCost.get(1))));
  }

  @Test
  void testWrapCostBaseOnNode() {
    var assignor = new CostAwareAssignor();
    var clusterInfo = buildClusterInfo();
    var topics = Set.of("a", "b", "c");
    var cost = new HashMap<TopicPartition, Double>();
    var rand = new Random();
    IntStream.range(0, 9)
        .forEach(
            i -> {
              cost.put(TopicPartition.of("a", i), rand.nextDouble());
              cost.put(TopicPartition.of("b", i), rand.nextDouble());
              cost.put(TopicPartition.of("c", i), rand.nextDouble());
            });

    var brokerTp = assignor.wrapCostBaseOnNode(clusterInfo, topics, cost);
    brokerTp.forEach((id, tps) -> Assertions.assertEquals(9, tps.size()));
    clusterInfo
        .replicaStream()
        .forEach(
            r -> {
              var tps = brokerTp.get(r.nodeInfo().id());
              Assertions.assertTrue(tps.containsKey(r.topicPartition()));
            });
  }

  static ClusterInfo buildClusterInfo() {
    return ClusterInfoBuilder.builder()
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
  }

  static ClusterBean buildClusterBean() {
    return ClusterBean.of(
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
  }

  static ServerMetrics.Topic.Meter bandwidth(
      ServerMetrics.Topic metric, String topic, double fifteenRate) {
    var domainName = "kafka.server";
    var properties =
        Map.of("type", "BrokerTopicMetric", "topic", topic, "name", metric.metricName());
    var attributes = Map.<String, Object>of("FifteenMinuteRate", fifteenRate);
    return new ServerMetrics.Topic.Meter(new BeanObject(domainName, properties, attributes));
  }

  private static int getFactor(int partition) {
    if (partition > 2 && partition < 6) return 3;
    else if (partition >= 6) return 5;
    return 1;
  }
}
