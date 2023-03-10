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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.metrics.MBeanRegister;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StrictCostPartitionerPerfTest {

  private static AtomicLong createMetric(int brokerId) {
    var latency = new AtomicLong(100);
    MBeanRegister.local()
        .domainName("kafka.producer")
        .property("type", "producer-node-metrics")
        .property("node-id", String.valueOf(brokerId))
        .property("client-id", "xxxx")
        .attribute("request-latency-avg", Double.class, () -> (double) latency.get())
        .register();
    return latency;
  }

  @Test
  void test() {
    var node0 = NodeInfo.of(0, "node0", 2222);
    var node1 = NodeInfo.of(1, "node1", 2222);
    var node2 = NodeInfo.of(2, "node2", 2222);
    var clusterInfo =
        ClusterInfo.of(
            "fake",
            List.of(node0, node1, node2),
            Map.of(),
            List.of(
                Replica.builder()
                    .topic("topic")
                    .partition(0)
                    .nodeInfo(node0)
                    .path("/tmp/aa")
                    .buildLeader(),
                Replica.builder()
                    .topic("topic")
                    .partition(1)
                    .nodeInfo(node1)
                    .path("/tmp/aa")
                    .buildLeader(),
                Replica.builder()
                    .topic("topic")
                    .partition(2)
                    .nodeInfo(node2)
                    .path("/tmp/aa")
                    .buildLeader()));

    var node0Latency = createMetric(0);
    var node1Latency = createMetric(1);
    var node2Latency = createMetric(2);

    var key = "key".getBytes(StandardCharsets.UTF_8);
    var value = "value".getBytes(StandardCharsets.UTF_8);
    try (var partitioner = new StrictCostPartitioner()) {
      partitioner.configure(Configuration.of(Map.of("round.robin.lease", "2s")));

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
