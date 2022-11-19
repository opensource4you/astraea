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
package org.astraea.common.balancer.algorithms;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.FakeClusterInfo;
import org.astraea.common.cost.DecreasingCost;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class GreedyBalancerTest {

  @Test
  void testConfig() {
    Assertions.assertTrue(
        GreedyBalancer.ALL_CONFIGS.contains("shuffle.plan.generator.min.step"),
        "Config exists for backward compatability reason");
    Assertions.assertTrue(
        GreedyBalancer.ALL_CONFIGS.contains("shuffle.plan.generator.max.step"),
        "Config exists for backward compatability reason");
    Assertions.assertTrue(
        GreedyBalancer.ALL_CONFIGS.contains("iteration"),
        "Config exists for backward compatability reason");

    Assertions.assertEquals(
        GreedyBalancer.ALL_CONFIGS.size(),
        Utils.constants(GreedyBalancer.class, name -> name.endsWith("CONFIG")).size(),
        "No duplicate element");
  }

  @Test
  void testJmx() {
    var cost = new DecreasingCost(Configuration.of(Map.of()));
    var id = "TestJmx-" + UUID.randomUUID();
    var clusterInfo = FakeClusterInfo.of(5, 5, 5, 2);
    var balancer =
        Balancer.create(
            GreedyBalancer.class,
            AlgorithmConfig.builder()
                .executionId(id)
                .dataFolders(clusterInfo.dataDirectories())
                .clusterCost(cost)
                .config(GreedyBalancer.ITERATION_CONFIG, "100")
                .build());

    try (MBeanClient client = MBeanClient.local()) {
      IntStream.range(0, 10)
          .forEach(
              run -> {
                var plan = balancer.offer(clusterInfo, Duration.ofMillis(300));
                Assertions.assertTrue(plan.isPresent());
                var bean =
                    Assertions.assertDoesNotThrow(
                        () ->
                            client.queryBean(
                                BeanQuery.builder()
                                    .domainName("astraea.balancer")
                                    .property("id", id)
                                    .property("algorithm", GreedyBalancer.class.getSimpleName())
                                    .property("run", Integer.toString(run))
                                    .build()));
                Assertions.assertEquals("astraea.balancer", bean.domainName());
                Assertions.assertTrue(0 < (long) bean.attributes().get("Iteration"));
                Assertions.assertTrue(1.0 > (double) bean.attributes().get("MinCost"));
              });
    }
  }

  @Test
  void testMinLongDouble() {
    long nan = Double.doubleToRawLongBits(Double.NaN);
    long pInf = Double.doubleToRawLongBits(Double.POSITIVE_INFINITY);
    long nInf = Double.doubleToRawLongBits(Double.NEGATIVE_INFINITY);
    long one = Double.doubleToRawLongBits(1.0);
    long two = Double.doubleToRawLongBits(2.0);

    Assertions.assertEquals(nan, GreedyBalancer.minLongDouble(nan, nan));
    Assertions.assertEquals(pInf, GreedyBalancer.minLongDouble(pInf, nan));
    Assertions.assertEquals(pInf, GreedyBalancer.minLongDouble(nan, pInf));
    Assertions.assertEquals(nInf, GreedyBalancer.minLongDouble(nInf, nan));
    Assertions.assertEquals(nInf, GreedyBalancer.minLongDouble(nan, nInf));
    Assertions.assertEquals(nInf, GreedyBalancer.minLongDouble(nInf, pInf));
    Assertions.assertEquals(nInf, GreedyBalancer.minLongDouble(pInf, nInf));
    Assertions.assertEquals(one, GreedyBalancer.minLongDouble(one, one));
    Assertions.assertEquals(one, GreedyBalancer.minLongDouble(one, two));
    Assertions.assertEquals(one, GreedyBalancer.minLongDouble(two, one));
    Assertions.assertEquals(one, GreedyBalancer.minLongDouble(one, nan));
    Assertions.assertEquals(one, GreedyBalancer.minLongDouble(nan, one));
    Assertions.assertEquals(two, GreedyBalancer.minLongDouble(two, nan));
    Assertions.assertEquals(two, GreedyBalancer.minLongDouble(nan, two));
    Assertions.assertEquals(two, GreedyBalancer.minLongDouble(two, two));
    Assertions.assertEquals(nInf, GreedyBalancer.minLongDouble(one, nInf));
    Assertions.assertEquals(nInf, GreedyBalancer.minLongDouble(nInf, one));
    Assertions.assertEquals(one, GreedyBalancer.minLongDouble(one, pInf));
    Assertions.assertEquals(one, GreedyBalancer.minLongDouble(pInf, one));
  }
}
