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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.Replica;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.app.common.Utils;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.producer.Producer;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RebalanceAdminTest extends RequireBrokerCluster {

  @Test
  void alterReplicaPlacements() throws InterruptedException {
    try (var admin = Admin.of(bootstrapServers())) {
      var topic = "alterReplicaPlacements_" + Utils.randomString();
      var rebalanceAdmin = RebalanceAdmin.of(admin, Map::of, (ignore) -> true);
      var selectedLogFolder =
          logFolders().entrySet().stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      Map.Entry::getKey,
                      x ->
                          List.copyOf(x.getValue())
                              .get(ThreadLocalRandom.current().nextInt(0, x.getValue().size()))));

      admin.creator().topic(topic).numberOfPartitions(1).numberOfReplicas((short) 1).create();
      TimeUnit.SECONDS.sleep(2);

      rebalanceAdmin.alterReplicaPlacements(
          new TopicPartition(topic, 0),
          List.of(
              LogPlacement.of(0, selectedLogFolder.get(0)),
              LogPlacement.of(1, selectedLogFolder.get(1)),
              LogPlacement.of(2, selectedLogFolder.get(2))));
      TimeUnit.SECONDS.sleep(3);

      var replicas =
          admin.replicas(Set.of(topic)).entrySet().stream()
              .filter(x -> x.getKey().topic().equals(topic))
              .filter(x -> x.getKey().partition() == 0)
              .map(Map.Entry::getValue)
              .findFirst()
              .orElseThrow();

      Assertions.assertEquals(
          Set.of(0, 1, 2),
          replicas.stream().map(Replica::broker).collect(Collectors.toUnmodifiableSet()));
      List.of(0, 1, 2)
          .forEach(
              brokerId ->
                  Assertions.assertEquals(
                      selectedLogFolder.get(brokerId),
                      replicas.stream()
                          .filter(x -> x.broker() == brokerId)
                          .findFirst()
                          .map(Replica::path)
                          .orElseThrow()));
    }
  }

  @Test
  void syncingProgress() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      final var rebalanceAdmin = RebalanceAdmin.of(admin, Map::of, (ignore) -> true);

      final var name = "SyncingProgress_" + Utils.randomString(6);
      admin.creator().topic(name).numberOfPartitions(3).numberOfReplicas((short) 2).create();
      IntStream.range(0, 3)
          .forEach(
              partition ->
                  Utils.packException(
                      () -> {
                        try (var producer = Producer.of(bootstrapServers())) {
                          producer
                              .sender()
                              .topic(name)
                              .partition(partition)
                              .value(new byte[1024 * (partition + 1)])
                              .run()
                              .toCompletableFuture()
                              .get();
                        }
                      }));

      final var replicaMap = admin.replicas(Set.of(name));
      final var logs =
          (Set<TopicPartitionReplica>)
              replicaMap.entrySet().stream()
                  .flatMap(
                      entry -> {
                        var tp = entry.getKey();
                        var replicas = entry.getValue();

                        return replicas.stream()
                            .map(
                                log ->
                                    new TopicPartitionReplica(
                                        tp.topic(), tp.partition(), log.broker()));
                      })
                  .collect(Collectors.toUnmodifiableSet());

      logs.forEach(
          log -> {
            var syncingProgress = rebalanceAdmin.syncingProgress(log);

            Assertions.assertTrue(syncingProgress.synced());
            Assertions.assertEquals(log.brokerId(), syncingProgress.brokerId());
            Assertions.assertEquals(log.topic(), syncingProgress.topicPartition().topic());
            Assertions.assertEquals(log.partition(), syncingProgress.topicPartition().partition());
            Assertions.assertEquals(
                syncingProgress.leaderLogSize().orElseThrow(), syncingProgress.logSize());
            Assertions.assertEquals(1, syncingProgress.percentage());

            long expectedMinLogSize = 1024L * (log.partition()) + 1;
            // the log contain metadata and record content, it supposed to be bigger than the actual
            // data
            Assertions.assertTrue(expectedMinLogSize < syncingProgress.logSize());
          });
    }
  }

  @Test
  void clusterInfo() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      final var rebalanceAdmin = RebalanceAdmin.of(admin, Map::of, (ignore) -> true);
      final var clusterInfo = rebalanceAdmin.clusterInfo();
      Assertions.assertEquals(admin.topicNames(), clusterInfo.topics());

      final var name = "RebalanceAdminTest" + Utils.randomString(6);
      admin.creator().topic(name).numberOfPartitions(3).create();
      final var rebalanceAdmin1 = RebalanceAdmin.of(admin, Map::of, name::equals);
      final var clusterInfo1 = rebalanceAdmin1.clusterInfo();
      Assertions.assertEquals(Set.of(name), clusterInfo1.topics());
    }
  }

  @Test
  void refreshMetrics() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      final var next = new AtomicInteger();
      Supplier<Map<Integer, Collection<HasBeanObject>>> metricSource =
          () -> Map.of(next.get(), List.of());

      final var rebalanceAdmin = RebalanceAdmin.of(admin, metricSource, (ignore) -> true);
      final var clusterInfo = rebalanceAdmin.refreshMetrics(rebalanceAdmin.clusterInfo());

      Assertions.assertEquals(List.of(), clusterInfo.beans(0));
      next.incrementAndGet();
      Assertions.assertEquals(List.of(), clusterInfo.beans(1));
      next.incrementAndGet();
      Assertions.assertEquals(List.of(), clusterInfo.beans(2));
    }
  }

  @Test
  void waitLogSynced() throws InterruptedException {
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.of(bootstrapServers())) {
      var topic = "WaitLogSynced_" + Utils.randomString();
      var rebalanceAdmin = RebalanceAdmin.of(admin, Map::of, (ignore) -> true);
      var dummy = new byte[1024];

      admin.creator().topic(topic).numberOfPartitions(1).numberOfReplicas((short) 1).create();
      TimeUnit.SECONDS.sleep(2);

      // Send 32 MiB data
      IntStream.range(0, 1024 * 32)
          .mapToObj(
              i ->
                  producer
                      .sender()
                      .topic(topic)
                      .partition(0)
                      .value(dummy)
                      .run()
                      .toCompletableFuture())
          .forEach(CompletableFuture::join);
      TimeUnit.SECONDS.sleep(1);

      admin.migrator().partition(topic, 0).moveTo(List.copyOf(brokerIds()));

      long time0 = System.currentTimeMillis();
      brokerIds()
          .forEach(
              id ->
                  Utils.packException(
                      () -> rebalanceAdmin.waitLogSynced(new TopicPartitionReplica(topic, 0, id))));
      long time1 = System.currentTimeMillis();

      Assertions.assertTrue(
          admin.replicas(Set.of(topic)).entrySet().stream()
              .flatMap(x -> x.getValue().stream())
              .allMatch(Replica::inSync));
      Assertions.assertTrue((time1 - time0) > 100, "This should takes awhile");
    }
  }

  @Test
  void waitPreferredLeaderSynced() throws InterruptedException {
    try (var admin = Admin.of(bootstrapServers())) {
      var topic = "WaitPreferredLeaderSynced_" + Utils.randomString();
      var rebalanceAdmin = RebalanceAdmin.of(admin, Map::of, (ignore) -> true);

      admin.creator().topic(topic).numberOfPartitions(1).numberOfReplicas((short) 3).create();
      TimeUnit.SECONDS.sleep(2);

      var currentLeader =
          (Supplier<Integer>)
              () ->
                  admin.replicas(Set.of(topic)).entrySet().stream()
                      .filter(x -> x.getKey().topic().equals(topic))
                      .filter(x -> x.getKey().partition() == 0)
                      .flatMap(x -> x.getValue().stream())
                      .filter(Replica::leader)
                      .findFirst()
                      .orElseThrow()
                      .broker();

      int oldLeader = currentLeader.get();

      // change the preferred leader
      int newPreferredLeader = (oldLeader + 2) % 3;
      admin
          .migrator()
          .partition(topic, 0)
          .moveTo(List.of(newPreferredLeader, (oldLeader + 1) % 3, oldLeader));

      // assert not leader yet
      Assertions.assertNotEquals(newPreferredLeader, currentLeader.get());

      // do election
      rebalanceAdmin.leaderElection(new TopicPartition(topic, 0));

      // wait for this
      Assertions.assertTrue(rebalanceAdmin.waitPreferredLeaderSynced(new TopicPartition(topic, 0)));

      // assert it is the leader
      Assertions.assertEquals(newPreferredLeader, currentLeader.get());
    }
  }

  @Test
  void leaderElection() throws InterruptedException {
    // this test can guard leaderElection too.
    waitPreferredLeaderSynced();
  }

  @Test
  void await() throws InterruptedException {
    // true
    Assertions.assertTrue(RebalanceAdminImpl.await(() -> true, Duration.ofSeconds(1)));

    // false
    Assertions.assertFalse(RebalanceAdminImpl.await(() -> false, Duration.ofSeconds(1)));

    // no matter what the timeout value is, it should try at least once.
    Assertions.assertTrue(RebalanceAdminImpl.await(() -> true, Duration.ZERO));
    Assertions.assertTrue(RebalanceAdminImpl.await(() -> true, Duration.ofSeconds(-1)));

    // no exception swallow
    Assertions.assertThrows(
        AssertionError.class,
        () ->
            RebalanceAdminImpl.await(
                () -> {
                  throw new AssertionError();
                },
                Duration.ofSeconds(1)));

    // forever works
    Assertions.assertDoesNotThrow(
        () -> RebalanceAdminImpl.await(() -> true, ChronoUnit.FOREVER.getDuration()));

    // timeout works
    {
      long now0 = System.currentTimeMillis();
      RebalanceAdminImpl.await(() -> false, Duration.ofMillis(1234));
      long now1 = System.currentTimeMillis();
      Assertions.assertTrue((now1 - now0) >= 1234);
    }

    // timeout works
    {
      long now0 = System.currentTimeMillis();
      RebalanceAdminImpl.await(
          () -> {
            long now1 = System.currentTimeMillis();
            // work done after 1 seconds
            return (now0 + 1000 < now1);
          },
          Duration.ofSeconds(7));
      long now2 = System.currentTimeMillis();
      Assertions.assertTrue(5000 >= (now2 - now0) && (now2 - now0) >= 1000);
    }
  }
}
