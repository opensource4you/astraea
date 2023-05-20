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
package org.astraea.common.partitioner;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.client.HasNodeMetrics;
import org.astraea.common.metrics.collector.MetricStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class StrictCostPartitionerPerfTest {

  private static BeanObject getBeanObject(int brokerId, double latency) {
    return new BeanObject(
        "kafka.producer",
        Map.of(
            "type", "producer-node-metrics",
            "node-id", String.valueOf(brokerId),
            "client-id", "xxxx"),
        Map.of("request-latency-avg", latency));
  }

  @Test
  void test() {
    var node0 = Broker.of(0, "node0", 2222);
    var node1 = Broker.of(1, "node1", 2222);
    var node2 = Broker.of(2, "node2", 2222);
    var clusterInfo =
        ClusterInfo.of(
            "fake",
            List.of(node0, node1, node2),
            Map.of(),
            List.of(
                Replica.builder()
                    .topic("topic")
                    .partition(0)
                    .broker(node0)
                    .path("/tmp/aa")
                    .buildLeader(),
                Replica.builder()
                    .topic("topic")
                    .partition(1)
                    .broker(node1)
                    .path("/tmp/aa")
                    .buildLeader(),
                Replica.builder()
                    .topic("topic")
                    .partition(2)
                    .broker(node2)
                    .path("/tmp/aa")
                    .buildLeader()));
    var admin = Mockito.mock(Admin.class);
    Mockito.when(admin.brokers())
        .thenReturn(CompletableFuture.completedStage(clusterInfo.brokers()));
    Mockito.when(admin.clusterInfo(Mockito.anySet()))
        .thenReturn(CompletableFuture.completedStage(clusterInfo));

    var node0Latency = new AtomicLong(100);
    var node1Latency = new AtomicLong(100);
    var node2Latency = new AtomicLong(100);

    var metricStore = Mockito.mock(MetricStore.class);
    Mockito.when(metricStore.clusterBean())
        .thenReturn(
            ClusterBean.of(
                Map.of(
                    -1,
                    List.of(
                        (HasNodeMetrics) () -> getBeanObject(0, node0Latency.get()),
                        () -> getBeanObject(1, node1Latency.get()),
                        () -> getBeanObject(2, node2Latency.get())))));

    var key = "key".getBytes(StandardCharsets.UTF_8);
    var value = "value".getBytes(StandardCharsets.UTF_8);
    try (var partitioner = new StrictCostPartitioner()) {
      partitioner.admin = admin;
      partitioner.configure(new Configuration(Map.of("round.robin.lease", "2s")));
      partitioner.metricStore = metricStore;

      Supplier<Map<Integer, List<Integer>>> resultSupplier =
          () -> {
            var result =
                IntStream.range(0, 1000)
                    .mapToObj(ignored -> partitioner.partition("topic", key, value, clusterInfo))
                    .collect(Collectors.groupingBy(i -> i));

            var keys =
                Arrays.stream(partitioner.roundRobinKeeper.roundRobin)
                    .boxed()
                    .collect(Collectors.groupingBy(i -> i))
                    .keySet();
            Assertions.assertEquals(3, keys.size(), "keys: " + keys);

            Assertions.assertEquals(3, result.size());
            result.values().forEach(v -> Assertions.assertNotEquals(0, v.size()));
            return result;
          };

      // first run
      node0Latency.set(100);
      node1Latency.set(10);
      node2Latency.set(1);
      Utils.sleep(Duration.ofSeconds(2));

      var result = resultSupplier.get();

      Assertions.assertTrue(
          result.get(2).size() > result.get(1).size(),
          "broker 0: "
              + result.get(0).size()
              + " broker 1: "
              + result.get(1).size()
              + " broker 2: "
              + result.get(2).size());

      Assertions.assertTrue(
          result.get(1).size() > result.get(0).size(),
          "broker 0: "
              + result.get(0).size()
              + " broker 1: "
              + result.get(1).size()
              + " broker 2: "
              + result.get(2).size());

      // second run
      node0Latency.set(77);
      node1Latency.set(3);
      node2Latency.set(999);
      Utils.sleep(Duration.ofSeconds(3));

      result = resultSupplier.get();
      Assertions.assertTrue(
          result.get(1).size() > result.get(0).size(),
          "broker 0: "
              + result.get(0).size()
              + " broker 1: "
              + result.get(1).size()
              + " broker 2: "
              + result.get(2).size());

      Assertions.assertTrue(
          result.get(0).size() > result.get(2).size(),
          "broker 0: "
              + result.get(0).size()
              + " broker 1: "
              + result.get(1).size()
              + " broker 2: "
              + result.get(2).size());
    }
  }
}
