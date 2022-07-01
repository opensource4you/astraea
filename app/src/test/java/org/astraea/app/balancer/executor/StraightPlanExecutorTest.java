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
package org.astraea.app.balancer.executor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.log.LayeredClusterLogAllocation;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.app.common.Utils;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StraightPlanExecutorTest extends RequireBrokerCluster {

  @Test
  void testRun() throws InterruptedException {
    // arrange
    try (Admin admin = Admin.of(bootstrapServers())) {
      final var topicName = "StraightPlanExecutorTest_" + Utils.randomString(8);
      admin.creator().topic(topicName).numberOfPartitions(10).numberOfReplicas((short) 2).create();
      final var originalAllocation =
          LayeredClusterLogAllocation.of(admin.clusterInfo(Set.of(topicName)));
      TimeUnit.SECONDS.sleep(3);
      final var broker0 = 0;
      final var broker1 = 1;
      final var logFolder0 = logFolders().get(broker0).stream().findAny().orElseThrow();
      final var logFolder1 = logFolders().get(broker1).stream().findAny().orElseThrow();
      final var onlyPlacement =
          List.of(LogPlacement.of(broker0, logFolder0), LogPlacement.of(broker1, logFolder1));
      final var allocationMap =
          IntStream.range(0, 10)
              .mapToObj(i -> new TopicPartition(topicName, i))
              .collect(Collectors.toUnmodifiableMap(tp -> tp, tp -> onlyPlacement));
      final var expectedAllocation = LayeredClusterLogAllocation.of(allocationMap);
      final var expectedTopicPartition =
          expectedAllocation.topicPartitionStream().collect(Collectors.toUnmodifiableSet());
      final var rebalanceAdmin = RebalanceAdmin.of(admin, Map::of, (ignore) -> true);

      // act
      final var result = new StraightPlanExecutor().run(rebalanceAdmin, expectedAllocation);

      // assert
      final var currentAllocation =
          LayeredClusterLogAllocation.of(admin.clusterInfo(Set.of(topicName)));
      final var currentTopicPartition =
          currentAllocation.topicPartitionStream().collect(Collectors.toUnmodifiableSet());
      Assertions.assertTrue(
          result.isDone(),
          () ->
              result
                  .exception()
                  .map(
                      ex -> {
                        StringWriter sw = new StringWriter();
                        ex.printStackTrace(new PrintWriter(sw));
                        return sw.toString();
                      })
                  .orElse("Failed with unknown reason"));
      Assertions.assertEquals(expectedTopicPartition, currentTopicPartition);
      expectedTopicPartition.forEach(
          topicPartition ->
              Assertions.assertEquals(
                  expectedAllocation.logPlacements(topicPartition),
                  currentAllocation.logPlacements(topicPartition),
                  "Testing for " + topicPartition));

      System.out.println("Expected:");
      System.out.println(ClusterLogAllocation.describeAllocation(expectedAllocation));
      System.out.println("Current:");
      System.out.println(ClusterLogAllocation.describeAllocation(currentAllocation));
      System.out.println("Original:");
      System.out.println(ClusterLogAllocation.describeAllocation(originalAllocation));
    }
  }
}
