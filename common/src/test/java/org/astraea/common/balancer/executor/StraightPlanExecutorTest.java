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
package org.astraea.common.balancer.executor;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ClusterInfoTest;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class StraightPlanExecutorTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testAsyncRun() {
    try (Admin admin = Admin.of(SERVICE.bootstrapServers())) {
      final var topicName = "StraightPlanExecutorTest_" + Utils.randomString(8);

      admin
          .creator()
          .topic(topicName)
          .numberOfPartitions(10)
          .numberOfReplicas((short) 2)
          .run()
          .toCompletableFuture()
          .join();

      Utils.sleep(Duration.ofSeconds(2));

      var originalAllocation = admin.clusterInfo(Set.of(topicName)).toCompletableFuture().join();

      Utils.sleep(Duration.ofSeconds(3));

      final var broker0 = 0;
      final var broker1 = 1;
      final var logFolder0 = SERVICE.dataFolders().get(broker0).stream().findAny().orElseThrow();
      final var logFolder1 = SERVICE.dataFolders().get(broker1).stream().findAny().orElseThrow();
      final var onlyPlacement =
          (Function<TopicPartition, List<Replica>>)
              (TopicPartition tp) ->
                  List.of(
                      Replica.builder()
                          .topic(tp.topic())
                          .partition(tp.partition())
                          .broker(Broker.of(broker0, "", -1))
                          .lag(0)
                          .size(0)
                          .isLeader(true)
                          .isSync(true)
                          .isFuture(false)
                          .isOffline(false)
                          .isPreferredLeader(true)
                          .path(logFolder0)
                          .build(),
                      Replica.builder()
                          .topic(tp.topic())
                          .partition(tp.partition())
                          .broker(Broker.of(broker1, "", -1))
                          .lag(0)
                          .size(0)
                          .isLeader(false)
                          .isSync(true)
                          .isFuture(false)
                          .isOffline(false)
                          .isPreferredLeader(false)
                          .path(logFolder1)
                          .build());
      final var allocation =
          IntStream.range(0, 10)
              .mapToObj(i -> TopicPartition.of(topicName, i))
              .collect(Collectors.toUnmodifiableMap(tp -> tp, onlyPlacement))
              .values()
              .stream()
              .flatMap(Collection::stream)
              .collect(Collectors.toUnmodifiableList());
      final var expectedAllocation = ClusterInfoTest.of(allocation);
      final var expectedTopicPartition = expectedAllocation.topicPartitions();

      var execute =
          new StraightPlanExecutor(
                  new Configuration(
                      Map.of(StraightPlanExecutor.CONFIG_ENABLE_DATA_DIRECTORY_MIGRATION, "true")))
              .run(admin, expectedAllocation, Duration.ofSeconds(10));

      execute.toCompletableFuture().join();

      final var CurrentAllocation =
          admin.clusterInfo(Set.of(topicName)).toCompletableFuture().join();

      final var CurrentTopicPartition = CurrentAllocation.topicPartitions();

      System.out.println("Expected:");
      System.out.println(ClusterInfo.toString(expectedAllocation));
      System.out.println("Current:");
      System.out.println(ClusterInfo.toString(CurrentAllocation));
      System.out.println("Original:");
      System.out.println(ClusterInfo.toString(originalAllocation));

      Assertions.assertEquals(expectedTopicPartition, CurrentTopicPartition);
      expectedTopicPartition.forEach(
          topicPartition ->
              Assertions.assertTrue(
                  ClusterInfo.placementMatch(
                      expectedAllocation.replicas(topicPartition),
                      CurrentAllocation.replicas(topicPartition))));
    }
  }

  @Test
  void testDisableDataDirMigration() {
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      var topic = Utils.randomString();
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(30)
          .numberOfReplicas((short) 1)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofMillis(300));

      var source = admin.clusterInfo(Set.of(topic)).toCompletableFuture().join();
      var target =
          ClusterInfo.builder(source)
              .mapLog(
                  replica ->
                      Replica.builder(replica)
                          .path(
                              source.brokerFolders().get(replica.broker().id()).stream()
                                  .filter(p -> !replica.path().equals(p))
                                  .findAny()
                                  .orElseThrow())
                          .build())
              .build();

      var spiedAdmin = Mockito.spy(admin);
      var executor =
          new StraightPlanExecutor(
              new Configuration(
                  Map.of(StraightPlanExecutor.CONFIG_ENABLE_DATA_DIRECTORY_MIGRATION, "false")));

      executor.run(spiedAdmin, target, Duration.ofSeconds(30)).toCompletableFuture().join();

      Mockito.verify(spiedAdmin, Mockito.never()).moveToFolders(Mockito.anyMap());
    }
  }
}
