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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.Replica;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.app.common.DataSize;
import org.astraea.app.common.DataUnit;
import org.astraea.app.common.Utils;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.jmx.BeanObject;
import org.astraea.app.producer.Producer;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RebalanceAdminTest extends RequireBrokerCluster {

  @Test
  void alterReplicaPlacementByList() throws InterruptedException {
    // arrange
    try (var admin = Admin.of(bootstrapServers())) {
      var topic = prepareTopic(admin, 1, (short) 1);
      var rebalanceAdmin = prepareRebalanceAdmin(admin);

      // scale the replica size from 1 to 3, to the following data dir
      var logFolder0 = randomElement(logFolders().get(0));
      var logFolder1 = randomElement(logFolders().get(1));
      var logFolder2 = randomElement(logFolders().get(2));

      // act
      var tasks =
          rebalanceAdmin.alterReplicaPlacements(
              new TopicPartition(topic, 0),
              List.of(
                  LogPlacement.of(0, logFolder0),
                  LogPlacement.of(1, logFolder1),
                  LogPlacement.of(2, logFolder2)));
      tasks.forEach(task -> task.await(Duration.ofSeconds(5)));

      // assert
      var topicPartition = new TopicPartition(topic, 0);
      var replicas = admin.replicas(Set.of(topic)).get(topicPartition);

      Assertions.assertEquals(
          List.of(0, 1, 2), replicas.stream().map(Replica::broker).collect(Collectors.toList()));
      Assertions.assertEquals(logFolder0, replicas.get(0).path());
      Assertions.assertEquals(logFolder1, replicas.get(1).path());
      Assertions.assertEquals(logFolder2, replicas.get(2).path());

      for (int i = 0; i < 3; i++) {
        var task = tasks.get(i);
        Assertions.assertTrue(task.progress().synced());
        Assertions.assertEquals(i, task.progress().brokerId());
        Assertions.assertEquals(1, task.progress().percentage());
        Assertions.assertTrue(task.await());
        Assertions.assertEquals(new TopicPartitionReplica(topic, 0, i), task.info());
      }
    }
  }

  // TODO: add a test alterReplicaPlacementByDir, and assert the syncing progress works

  @Test
  void syncingProgress() throws InterruptedException {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = prepareTopic(admin, 3, (short) 2);
      var rebalanceAdmin = prepareRebalanceAdmin(admin);

      prepareData(topic, 0, DataUnit.KiB.of(1024));
      prepareData(topic, 1, DataUnit.KiB.of(2048));
      prepareData(topic, 2, DataUnit.KiB.of(3072));

      final var replicaMap = admin.replicas(Set.of(topic));
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

            // assert synced
            Assertions.assertTrue(syncingProgress.synced());

            // assert topic/partition/replica correct
            Assertions.assertEquals(log.topic(), syncingProgress.topicPartition().topic());
            Assertions.assertEquals(log.partition(), syncingProgress.topicPartition().partition());
            Assertions.assertEquals(log.brokerId(), syncingProgress.brokerId());

            // assert log size correct
            Assertions.assertEquals(
                syncingProgress.leaderLogSize().orElseThrow(), syncingProgress.logSize());

            // assert percentage ok
            Assertions.assertEquals(1, syncingProgress.percentage());

            // assert size
            long expectedMinLogSize = 1024L * (log.partition()) + 1;
            // log contain metadata and record content, it supposed to be bigger than the actual
            // data
            Assertions.assertTrue(expectedMinLogSize < syncingProgress.logSize());
          });
    }
  }

  @Test
  void clusterInfo() throws InterruptedException {
    try (Admin admin = Admin.of(bootstrapServers())) {
      // test if all topics are covered
      final var rebalanceAdmin = RebalanceAdmin.of(admin, Map::of, (ignore) -> true);
      final var clusterInfo = rebalanceAdmin.clusterInfo();
      Assertions.assertEquals(admin.topicNames(), clusterInfo.topics());

      // create topic
      final var topic = prepareTopic(admin, 3, (short) 1);

      // test if topic filter works
      final var rebalanceAdmin1 = RebalanceAdmin.of(admin, Map::of, topic::equals);
      final var clusterInfo1 = rebalanceAdmin1.clusterInfo();
      Assertions.assertEquals(Set.of(topic), clusterInfo1.topics());
    }
  }

  @Test
  void refreshMetrics() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      final var next = new AtomicInteger();
      final var idBean =
          (Function<Integer, BeanObject>)
              (i) -> new BeanObject(Integer.toString(i), Map.of(), Map.of());
      Supplier<Map<Integer, Collection<HasBeanObject>>> metricSource =
          () ->
              Map.of(
                  0, List.of(() -> idBean.apply(next.get())),
                  1, List.of(() -> idBean.apply(next.get())),
                  2, List.of(() -> idBean.apply(next.get())));
      BiFunction<ClusterInfo, Integer, BeanObject> firstBeanObject =
          (clusterInfo, broker) ->
              clusterInfo.clusterBean().all().get(broker).iterator().next().beanObject();

      final var rebalanceAdmin = RebalanceAdmin.of(admin, metricSource, (ignore) -> true);

      var clusterInfo = rebalanceAdmin.refreshMetrics(rebalanceAdmin.clusterInfo());
      Assertions.assertEquals("0", firstBeanObject.apply(clusterInfo, 0).domainName());
      Assertions.assertEquals("0", firstBeanObject.apply(clusterInfo, 1).domainName());
      Assertions.assertEquals("0", firstBeanObject.apply(clusterInfo, 2).domainName());
      next.incrementAndGet();
      clusterInfo = rebalanceAdmin.refreshMetrics(rebalanceAdmin.clusterInfo());
      Assertions.assertEquals("1", firstBeanObject.apply(clusterInfo, 0).domainName());
      Assertions.assertEquals("1", firstBeanObject.apply(clusterInfo, 1).domainName());
      Assertions.assertEquals("1", firstBeanObject.apply(clusterInfo, 2).domainName());
      next.incrementAndGet();
      clusterInfo = rebalanceAdmin.refreshMetrics(rebalanceAdmin.clusterInfo());
      Assertions.assertEquals("2", firstBeanObject.apply(clusterInfo, 0).domainName());
      Assertions.assertEquals("2", firstBeanObject.apply(clusterInfo, 1).domainName());
      Assertions.assertEquals("2", firstBeanObject.apply(clusterInfo, 2).domainName());
    }
  }

  @Test
  void waitLogSynced() throws InterruptedException {
    // arrange
    try (var admin = Admin.of(bootstrapServers())) {
      var topic = prepareTopic(admin, 1, (short) 1);
      var topicPartition = new TopicPartition(topic, 0);
      var rebalanceAdmin = prepareRebalanceAdmin(admin);
      var beginReplica = admin.replicas().get(topicPartition).get(0);
      var otherDataDir =
          admin.brokerFolders().get(beginReplica.broker()).stream()
              .filter(dir -> !dir.equals(beginReplica.path()))
              .findAny()
              .orElseThrow();
      prepareData(topic, 0, DataUnit.MiB.of(32));
      // let two brokers join the replica list
      admin.migrator().partition(topic, 0).moveTo(List.of(0, 1, 2));
      // let the existing replica change its directory
      // TODO: fix this test, the data directory migration doesn't work, but the debounceAwait with
      // high waiting time covered this error.
      admin.migrator().partition(topic, 0).moveTo(Map.of(beginReplica.broker(), otherDataDir));

      // act
      long time0 = System.currentTimeMillis();
      rebalanceAdmin.waitLogSynced(new TopicPartitionReplica(topic, 0, 0));
      rebalanceAdmin.waitLogSynced(new TopicPartitionReplica(topic, 0, 1));
      rebalanceAdmin.waitLogSynced(new TopicPartitionReplica(topic, 0, 2));
      long time1 = System.currentTimeMillis();

      // assert all replica synced
      Assertions.assertTrue(
          admin.replicas(Set.of(topic)).entrySet().stream()
              .flatMap(x -> x.getValue().stream())
              .allMatch(Replica::inSync));
      // assert all data directory migration synced
      Assertions.assertTrue(
          admin.replicas(Set.of(topic)).entrySet().stream()
              .flatMap(x -> x.getValue().stream())
              .noneMatch(Replica::isFuture));
      Assertions.assertTrue((time1 - time0) > 100, "This should takes awhile");
    }
  }

  @Test
  void waitPreferredLeaderSynced() throws InterruptedException {
    try (var admin = Admin.of(bootstrapServers())) {
      var topic = prepareTopic(admin, 1, (short) 3);
      var rebalanceAdmin = prepareRebalanceAdmin(admin);
      var topicPartition = new TopicPartition(topic, 0);

      var leaderNow =
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

      int oldLeader = leaderNow.get();

      // change the preferred leader
      int newPreferredLeader = (oldLeader + 2) % 3;
      admin
          .migrator()
          .partition(topic, 0)
          .moveTo(List.of(newPreferredLeader, (oldLeader + 1) % 3, oldLeader));

      // assert not leader yet
      Assertions.assertNotEquals(newPreferredLeader, leaderNow.get());

      // do election
      rebalanceAdmin.leaderElection(topicPartition);

      // wait for this
      Assertions.assertTrue(rebalanceAdmin.waitPreferredLeaderSynced(topicPartition));

      // assert it is the leader
      Assertions.assertEquals(newPreferredLeader, leaderNow.get());
    }
  }

  @Test
  void leaderElection() throws InterruptedException {
    try (var admin = Admin.of(bootstrapServers())) {
      var topic = prepareTopic(admin, 1, (short) 3);
      var rebalanceAdmin = prepareRebalanceAdmin(admin);
      var topicPartition = new TopicPartition(topic, 0);

      var leaderNow =
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

      int oldLeader = leaderNow.get();

      // change the preferred leader
      int newPreferredLeader = (oldLeader + 2) % 3;
      admin
          .migrator()
          .partition(topic, 0)
          .moveTo(List.of(newPreferredLeader, (oldLeader + 1) % 3, oldLeader));

      // assert not leader yet
      Assertions.assertNotEquals(newPreferredLeader, leaderNow.get());

      // do election
      var task = rebalanceAdmin.leaderElection(topicPartition);

      // wait for this
      Assertions.assertTrue(rebalanceAdmin.waitPreferredLeaderSynced(topicPartition));

      // the task object works
      Assertions.assertTrue(task.progress());
      Assertions.assertTrue(task.await());

      // assert it is the leader now
      Assertions.assertEquals(newPreferredLeader, leaderNow.get());
    }
  }

  @Test
  void debouncedAwait() throws InterruptedException {
    Assertions.assertTrue(
        RebalanceAdminImpl.debouncedAwait(
            () -> true, Duration.ofSeconds(1), Duration.ofSeconds(3)));
    Assertions.assertFalse(
        RebalanceAdminImpl.debouncedAwait(
            () -> false, Duration.ofSeconds(1), Duration.ofSeconds(3)));

    Assertions.assertTrue(
        RebalanceAdminImpl.debouncedAwait(
            new Supplier<>() {
              final AtomicInteger i = new AtomicInteger(0);

              @Override
              public Boolean get() {
                return i.getAndIncrement() > 0;
              }
            },
            Duration.ofMillis(500),
            Duration.ofSeconds(3)));
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

  String prepareTopic(Admin admin, int partition, short replica) throws InterruptedException {
    var topicName = Utils.randomString();

    admin
        .creator()
        .topic(topicName)
        .numberOfPartitions(partition)
        .numberOfReplicas(replica)
        .create();
    TimeUnit.SECONDS.sleep(1);

    return topicName;
  }

  RebalanceAdmin prepareRebalanceAdmin(Admin admin) {
    return RebalanceAdmin.of(admin, Map::of, (ignore) -> true);
  }

  void prepareData(String topic, int partition, DataSize dataSize) {
    var dummy = new byte[1024];
    int sends = dataSize.measurement(DataUnit.KiB).intValue();
    try (var producer = Producer.of(bootstrapServers())) {
      var sender = producer.sender().topic(topic).partition(partition).value(dummy);
      IntStream.range(0, sends)
          .parallel()
          .mapToObj(i -> sender.run().toCompletableFuture())
          .collect(Collectors.toSet())
          .forEach(CompletableFuture::join);
    }
  }

  <T> T randomElement(Collection<T> collection) {
    return collection.stream()
        .skip(ThreadLocalRandom.current().nextInt(0, collection.size()))
        .findFirst()
        .orElseThrow();
  }
}
