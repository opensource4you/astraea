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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.astraea.app.admin.Admin;
import org.astraea.app.admin.Replica;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
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
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

class RebalanceAdminTest extends RequireBrokerCluster {

  @Test
  void alterReplicaPlacementByList() throws InterruptedException {
    // arrange
    try (var admin = Admin.of(bootstrapServers())) {
      var topic = prepareTopic(admin, 1, (short) 1);
      var rebalanceAdmin = prepareRebalanceAdmin(admin);
      RebalanceAdminImpl.changeRetrialTime(Duration.ofMillis(50));

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
      tasks.forEach(
          task -> Utils.packException(() -> task.completableFuture().get(5, TimeUnit.SECONDS)));

      // assert
      var topicPartition = new TopicPartition(topic, 0);
      var replicas = admin.replicas(Set.of(topic)).get(topicPartition);

      Assertions.assertEquals(
          List.of(0, 1, 2), replicas.stream().map(Replica::broker).collect(Collectors.toList()));
      Assertions.assertEquals(logFolder0, replicas.get(0).path());
      Assertions.assertEquals(logFolder1, replicas.get(1).path());
      Assertions.assertEquals(logFolder2, replicas.get(2).path());
    }
  }

  // repeat the test so it has higher chance to fail
  @RepeatedTest(value = 3)
  void alterReplicaPlacementByDirectory() throws InterruptedException {
    // arrange
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = prepareTopic(admin, 1, (short) 1);
      var topicPartition = new TopicPartition(topic, 0);
      var rebalanceAdmin = prepareRebalanceAdmin(admin);
      // decrease the debouncing time so the test has higher chance to fail
      RebalanceAdminImpl.changeRetrialTime(Duration.ofMillis(150));
      prepareData(topic, 0, DataUnit.MiB.of(256));
      Supplier<Replica> replicaNow = () -> admin.replicas(Set.of(topic)).get(topicPartition).get(0);
      var originalReplica = replicaNow.get();
      var nextDir =
          logFolders().get(originalReplica.broker()).stream()
              .filter(name -> !name.equals(originalReplica.path()))
              .findAny()
              .orElseThrow();
      var expectedPlacement = LogPlacement.of(originalReplica.broker(), nextDir);

      // act, change the dir of the only replica
      var task =
          rebalanceAdmin.alterReplicaPlacements(topicPartition, List.of(expectedPlacement)).get(0);

      // assert
      task.completableFuture().join();
      var finalReplica = replicaNow.get();
      Assertions.assertTrue(finalReplica.inSync());
      Assertions.assertFalse(finalReplica.isFuture());
      Assertions.assertEquals(originalReplica.broker(), finalReplica.broker());
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
  void checkLogSynced() throws InterruptedException, ExecutionException {
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
      RebalanceAdminImpl.changeRetrialTime(Duration.ofMillis(150));
      prepareData(topic, 0, DataUnit.MiB.of(32));
      // let two brokers join the replica list
      admin.migrator().partition(topic, 0).moveTo(List.of(0, 1, 2));
      // let the existing replica change its directory
      admin.migrator().partition(topic, 0).moveTo(Map.of(beginReplica.broker(), otherDataDir));

      // act
      long time0 = System.currentTimeMillis();
      rebalanceAdmin.checkLogSynced(new TopicPartitionReplica(topic, 0, 0)).get();
      rebalanceAdmin.checkLogSynced(new TopicPartitionReplica(topic, 0, 1)).get();
      rebalanceAdmin.checkLogSynced(new TopicPartitionReplica(topic, 0, 2)).get();
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
  void checkPreferredLeaderSynced()
      throws InterruptedException, ExecutionException, TimeoutException {
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
      rebalanceAdmin.leaderElection(topicPartition).completableFuture().get();

      // wait for this
      rebalanceAdmin.checkPreferredLeaderSynced(topicPartition).get(5, TimeUnit.SECONDS);

      // assert it is the leader
      Assertions.assertEquals(newPreferredLeader, leaderNow.get());
    }
  }

  @Test
  void leaderElection() throws InterruptedException, ExecutionException, TimeoutException {
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
      rebalanceAdmin.checkPreferredLeaderSynced(topicPartition).get(5, TimeUnit.SECONDS);

      // assert it is the leader now
      Assertions.assertEquals(newPreferredLeader, leaderNow.get());
    }
  }

  @Test
  void testTopicFilter() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic1 = Utils.randomString();
      var topic2 = Utils.randomString();
      var topic3 = Utils.randomString();
      var topicPartition1 = new TopicPartition(topic1, 0);
      var topicPartition2 = new TopicPartition(topic2, 0);
      var topicPartition3 = new TopicPartition(topic3, 0);
      Stream.of(topic1, topic2, topic3)
          .forEach(i -> admin.creator().topic(i).numberOfPartitions(1).create());
      var allowed = List.of(topic1, topic2);
      Predicate<String> filter = allowed::contains;
      var rebalanceAdmin = RebalanceAdmin.of(admin, Map::of, filter);

      Assertions.assertTrue(rebalanceAdmin.topicFilter().test(topic1));
      Assertions.assertTrue(rebalanceAdmin.topicFilter().test(topic2));
      Assertions.assertFalse(rebalanceAdmin.topicFilter().test(topic3));
      Assertions.assertFalse(rebalanceAdmin.topicFilter().test("Something"));
      Assertions.assertDoesNotThrow(() -> rebalanceAdmin.leaderElection(topicPartition1));
      Assertions.assertDoesNotThrow(() -> rebalanceAdmin.leaderElection(topicPartition2));
      Assertions.assertThrows(IllegalArgumentException.class, () -> rebalanceAdmin.leaderElection(topicPartition3));
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
