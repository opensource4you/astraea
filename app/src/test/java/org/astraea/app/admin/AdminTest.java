package org.astraea.app.admin;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.condition.OS.WINDOWS;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.config.TopicConfig;
import org.astraea.app.common.Utils;
import org.astraea.app.consumer.Consumer;
import org.astraea.app.consumer.Deserializer;
import org.astraea.app.producer.Producer;
import org.astraea.app.producer.Serializer;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class AdminTest extends RequireBrokerCluster {

  @Test
  void testCreator() {
    var topicName = "testCreator";
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topicName)
          .configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"))
          .create();
      Utils.waitFor(
          () ->
              admin
                  .topics()
                  .get(topicName)
                  .value(TopicConfig.COMPRESSION_TYPE_CONFIG)
                  .filter(value -> value.equals("lz4"))
                  .isPresent());

      var config = admin.topics().get(topicName);
      Assertions.assertEquals(
          config.keys().size(), (int) StreamSupport.stream(config.spliterator(), false).count());
      config.keys().forEach(key -> Assertions.assertTrue(config.value(key).isPresent()));
      Assertions.assertTrue(config.values().contains("lz4"));
    }
  }

  @Test
  void testCreateTopicRepeatedly() {
    var topicName = "testCreateTopicRepeatedly";
    try (var admin = Admin.of(bootstrapServers())) {
      Runnable createTopic =
          () ->
              admin
                  .creator()
                  .configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"))
                  .numberOfReplicas((short) 1)
                  .numberOfPartitions(3)
                  .topic(topicName)
                  .create();

      createTopic.run();
      Utils.waitFor(() -> admin.topics().containsKey(topicName));
      IntStream.range(0, 10).forEach(i -> createTopic.run());

      // changing number of partitions can producer error
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> admin.creator().numberOfPartitions(1).topic(topicName).create());

      // changing number of replicas can producer error
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> admin.creator().numberOfReplicas((short) 2).topic(topicName).create());

      // changing config can producer error
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              admin
                  .creator()
                  .configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip"))
                  .topic(topicName)
                  .create());
    }
  }

  @Test
  void testPartitions() throws InterruptedException {
    var topicName = "testPartitions";
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(3).create();
      // wait for syncing topic creation
      TimeUnit.SECONDS.sleep(5);
      Assertions.assertTrue(admin.topicNames().contains(topicName));
      var partitions = admin.replicas(Set.of(topicName));
      Assertions.assertEquals(3, partitions.size());
      var logFolders =
          logFolders().values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
      partitions
          .values()
          .forEach(
              replicas ->
                  replicas.forEach(
                      replica ->
                          Assertions.assertTrue(
                              logFolders.stream().anyMatch(replica.path()::contains))));
    }
  }

  @Test
  void testOffsets() throws InterruptedException {
    var topicName = "testOffsets";
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(3).create();
      // wait for syncing topic creation
      TimeUnit.SECONDS.sleep(3);
      var offsets = admin.offsets(Set.of(topicName));
      Assertions.assertEquals(3, offsets.size());
      offsets
          .values()
          .forEach(
              offset -> {
                Assertions.assertEquals(0, offset.earliest());
                Assertions.assertEquals(0, offset.latest());
              });

      admin.creator().topic("a").numberOfPartitions(3).create();
      // wait for syncing topic creation
      TimeUnit.SECONDS.sleep(3);
      Assertions.assertEquals(6, admin.offsets().size());
    }
  }

  @Test
  void testConsumerGroups() throws InterruptedException {
    var topicName = "testConsumerGroups-Topic";
    var consumerGroup = "testConsumerGroups-Group";
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(3).create();
      try (var c1 =
          Consumer.builder()
              .bootstrapServers(bootstrapServers())
              .topics(Set.of(topicName))
              .groupId(consumerGroup)
              .build()) {
        // wait for syncing topic creation
        TimeUnit.SECONDS.sleep(5);
        var consumerGroupMap = admin.consumerGroups(Set.of(consumerGroup));
        Assertions.assertEquals(1, consumerGroupMap.size());
        Assertions.assertTrue(consumerGroupMap.containsKey(consumerGroup));
        Assertions.assertEquals(consumerGroup, consumerGroupMap.get(consumerGroup).groupId());

        try (var c2 =
            Consumer.builder()
                .bootstrapServers(bootstrapServers())
                .topics(Set.of(topicName))
                .groupId("abc")
                .build()) {
          var count =
              admin.consumerGroupIds().stream()
                  .mapToInt(t -> admin.consumerGroups(Set.of(t)).size())
                  .sum();
          Assertions.assertEquals(count, admin.consumerGroups().size());
          Assertions.assertEquals(1, admin.consumerGroups(Set.of("abc")).size());
        }
      }
    }
  }

  @Test
  // There is a problem when migrating the log folder under Windows because the migrated source
  // cannot be deleted, so disabled this test on Windows for now.
  @DisabledOnOs(WINDOWS)
  void testMigrateSinglePartition() throws InterruptedException {
    var topicName = "testMigrateSinglePartition";
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(1).create();
      // wait for syncing topic creation
      TimeUnit.SECONDS.sleep(5);
      var broker = admin.brokerIds().iterator().next();
      admin.migrator().partition(topicName, 0).moveTo(List.of(broker));
      Utils.waitFor(
          () -> {
            var replicas = admin.replicas(Set.of(topicName));
            var partitionReplicas = replicas.entrySet().iterator().next().getValue();
            return replicas.size() == 1
                && partitionReplicas.size() == 1
                && partitionReplicas.get(0).broker() == broker;
          });

      var currentBroker =
          admin.replicas(Set.of(topicName)).get(new TopicPartition(topicName, 0)).get(0).broker();
      var allPath = admin.brokerFolders(Set.of(currentBroker));
      var otherPath =
          allPath.get(currentBroker).stream()
              .filter(
                  i ->
                      !i.contains(
                          admin
                              .replicas(Set.of(topicName))
                              .get(new TopicPartition(topicName, 0))
                              .get(0)
                              .path()))
              .collect(Collectors.toSet());
      admin
          .migrator()
          .partition(topicName, 0)
          .moveTo(Map.of(currentBroker, otherPath.iterator().next()));
      Utils.waitFor(
          () -> {
            var replicas = admin.replicas(Set.of(topicName));
            var partitionReplicas = replicas.entrySet().iterator().next().getValue();
            return replicas.size() == 1
                && partitionReplicas.size() == 1
                && partitionReplicas.get(0).path().equals(otherPath.iterator().next());
          });
    }
  }

  @Test
  @DisabledOnOs(WINDOWS)
  void testMigrateAllPartitions() throws InterruptedException {
    var topicName = "testMigrateAllPartitions";
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(3).create();
      // wait for syncing topic creation
      TimeUnit.SECONDS.sleep(5);
      var broker = admin.brokerIds().iterator().next();
      admin.migrator().topic(topicName).moveTo(List.of(broker));
      Utils.waitFor(
          () -> {
            var replicas = admin.replicas(Set.of(topicName));
            if (replicas.size() != 3) return false;
            if (!replicas.values().stream().allMatch(rs -> rs.size() == 1)) return false;
            return replicas.values().stream()
                .allMatch(rs -> rs.stream().allMatch(r -> r.broker() == broker));
          });
    }
  }

  @Test
  void testReplicaSize() throws ExecutionException, InterruptedException {
    var topicName = "testReplicaSize";
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.builder().bootstrapServers(bootstrapServers()).build()) {
      producer.sender().topic(topicName).key(new byte[100]).run().toCompletableFuture().get();
      var originSize =
          admin.replicas(Set.of(topicName)).entrySet().iterator().next().getValue().get(0).size();

      // add data again
      producer.sender().topic(topicName).key(new byte[100]).run().toCompletableFuture().get();

      var newSize =
          admin.replicas(Set.of(topicName)).entrySet().iterator().next().getValue().get(0).size();
      Assertions.assertTrue(newSize > originSize);
    }
  }

  @Test
  void testCompact() throws InterruptedException {
    var topicName = "testCompacted";
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).compactionMaxLag(Duration.ofSeconds(1)).create();

      var key = "key";
      var anotherKey = "anotherKey";
      var value = "value";
      try (var producer =
          Producer.builder()
              .keySerializer(Serializer.STRING)
              .valueSerializer(Serializer.STRING)
              .bootstrapServers(bootstrapServers())
              .build()) {
        IntStream.range(0, 10)
            .forEach(i -> producer.sender().key(key).value(value).topic(topicName).run());
        producer.flush();

        // sleep and produce more data to generate the new segment
        TimeUnit.SECONDS.sleep(2);
        IntStream.range(0, 10)
            .forEach(i -> producer.sender().key(anotherKey).value(value).topic(topicName).run());
        producer.flush();
      }

      // sleep for compact (the backoff of compact thread is reduced to 2 seconds.
      // see org.astraea.service.Services)
      TimeUnit.SECONDS.sleep(3);

      try (var consumer =
          Consumer.builder()
              .keyDeserializer(Deserializer.STRING)
              .valueDeserializer(Deserializer.STRING)
              .fromBeginning()
              .bootstrapServers(bootstrapServers())
              .topics(Set.of(topicName))
              .build()) {

        var records =
            IntStream.range(0, 5)
                .mapToObj(i -> consumer.poll(Duration.ofSeconds(1)))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        Assertions.assertEquals(
            1, records.stream().filter(record -> record.key().equals(key)).count());

        // those data are in active segment, so they can be compacted.
        Assertions.assertEquals(
            10, records.stream().filter(record -> record.key().equals(anotherKey)).count());
      }
    }
  }

  @Test
  void testBrokerConfigs() {
    try (var admin = Admin.of(bootstrapServers())) {
      var brokerConfigs = admin.brokers();
      Assertions.assertEquals(3, brokerConfigs.size());
      brokerConfigs.values().forEach(c -> Assertions.assertNotEquals(0, c.keys().size()));
      Assertions.assertEquals(1, admin.brokers(Set.of(brokerIds().iterator().next())).size());
    }
  }

  @Test
  void testBrokerFolders() {
    try (var admin = Admin.of(bootstrapServers())) {
      Assertions.assertEquals(brokerIds().size(), admin.brokerFolders().size());
      // list all
      logFolders()
          .forEach(
              (id, ds) -> Assertions.assertEquals(admin.brokerFolders().get(id).size(), ds.size()));

      // list folders one by one
      logFolders()
          .forEach(
              (id, ds) -> {
                Assertions.assertEquals(1, admin.brokerFolders(Set.of(id)).size());
                Assertions.assertEquals(admin.brokerFolders(Set.of(id)).get(id).size(), ds.size());
              });
    }
  }

  @Test
  void testReplicas() throws InterruptedException {
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic("abc").numberOfPartitions(2).create();
      TimeUnit.SECONDS.sleep(2);
      Assertions.assertEquals(2, admin.replicas(Set.of("abc")).size());

      var count = admin.topicNames().stream().mapToInt(t -> admin.replicas(Set.of(t)).size()).sum();
      Assertions.assertEquals(count, admin.replicas().size());
    }
  }

  @Test
  void testProducerStates() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    try (var producer = Producer.of(bootstrapServers());
        var admin = Admin.of(bootstrapServers())) {
      producer.sender().topic(topic).value(new byte[1]).run().toCompletableFuture().get();

      var states = admin.producerStates();
      Assertions.assertNotEquals(0, states.size());
      var producerState =
          states.entrySet().stream()
              .filter(tp -> tp.getKey().topic().equals(topic))
              .flatMap(e -> e.getValue().stream())
              .collect(Collectors.toUnmodifiableList());
      Assertions.assertEquals(1, producerState.size());
    }
  }

  @Test
  void testIpQuota() throws InterruptedException {
    try (var admin = Admin.of(bootstrapServers())) {
      admin.quotaCreator().ip("192.168.11.11").connectionRate(10).create();
      TimeUnit.SECONDS.sleep(2);

      java.util.function.Consumer<List<Quota>> checker =
          (quotas) -> {
            Assertions.assertEquals(1, quotas.size());
            Assertions.assertEquals(
                Set.of(Quota.Target.IP),
                quotas.stream().map(Quota::target).collect(Collectors.toSet()));
            Assertions.assertEquals(
                Set.of(Quota.Limit.IP_CONNECTION_RATE),
                quotas.stream().map(Quota::limit).collect(Collectors.toSet()));
            Assertions.assertEquals("192.168.11.11", quotas.iterator().next().targetValue());
            Assertions.assertEquals(10, quotas.iterator().next().limitValue());
          };

      // only target
      checker.accept(
          admin.quotas(Quota.Target.IP).stream().collect(Collectors.toUnmodifiableList()));

      // only target and name
      checker.accept(
          admin.quotas(Quota.Target.IP, "192.168.11.11").stream()
              .collect(Collectors.toUnmodifiableList()));
    }
  }

  @Test
  void testMultipleIpQuota() throws InterruptedException {
    try (var admin = Admin.of(bootstrapServers())) {
      admin.quotaCreator().ip("192.168.11.11").connectionRate(10).create();
      admin.quotaCreator().ip("192.168.11.11").connectionRate(12).create();
      admin.quotaCreator().ip("192.168.11.11").connectionRate(9).create();
      TimeUnit.SECONDS.sleep(2);
      Assertions.assertEquals(1, admin.quotas(Quota.Target.IP, "192.168.11.11").size());
    }
  }

  @Test
  void testClientQuota() throws InterruptedException {
    try (var admin = Admin.of(bootstrapServers())) {
      admin.quotaCreator().clientId("my-id").produceRate(10).consumeRate(100).create();
      TimeUnit.SECONDS.sleep(2);

      java.util.function.Consumer<List<Quota>> checker =
          (quotas) -> {
            Assertions.assertEquals(2, quotas.size());
            Assertions.assertEquals(
                Set.of(Quota.Target.CLIENT_ID),
                quotas.stream().map(Quota::target).collect(Collectors.toSet()));
            Assertions.assertEquals(
                Set.of(Quota.Limit.PRODUCER_BYTE_RATE, Quota.Limit.CONSUMER_BYTE_RATE),
                quotas.stream().map(Quota::limit).collect(Collectors.toSet()));
            Assertions.assertEquals(
                10,
                quotas.stream()
                    .filter(q -> q.limit() == Quota.Limit.PRODUCER_BYTE_RATE)
                    .findFirst()
                    .get()
                    .limitValue());
            Assertions.assertEquals(
                "my-id",
                quotas.stream()
                    .filter(q -> q.limit() == Quota.Limit.PRODUCER_BYTE_RATE)
                    .findFirst()
                    .get()
                    .targetValue());
            Assertions.assertEquals(
                100,
                quotas.stream()
                    .filter(q -> q.limit() == Quota.Limit.CONSUMER_BYTE_RATE)
                    .findFirst()
                    .get()
                    .limitValue());
            Assertions.assertEquals(
                "my-id",
                quotas.stream()
                    .filter(q -> q.limit() == Quota.Limit.CONSUMER_BYTE_RATE)
                    .findFirst()
                    .get()
                    .targetValue());
          };

      // only target
      checker.accept(
          admin.quotas(Quota.Target.CLIENT_ID).stream().collect(Collectors.toUnmodifiableList()));

      // only target and name
      checker.accept(
          admin.quotas(Quota.Target.CLIENT_ID, "my-id").stream()
              .collect(Collectors.toUnmodifiableList()));
    }
  }

  @Test
  void testMultipleClientQuota() throws InterruptedException {
    try (var admin = Admin.of(bootstrapServers())) {
      admin.quotaCreator().clientId("my-id").consumeRate(100).create();
      admin.quotaCreator().clientId("my-id").produceRate(999).create();
      TimeUnit.SECONDS.sleep(2);
      Assertions.assertEquals(2, admin.quotas(Quota.Target.CLIENT_ID, "my-id").size());
    }
  }

  @Test
  void somePartitionsOffline() {
    String topicName1 = "testOfflineTopic-1";
    String topicName2 = "testOfflineTopic-2";
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName1).numberOfPartitions(4).numberOfReplicas((short) 1).create();
      admin.creator().topic(topicName2).numberOfPartitions(4).numberOfReplicas((short) 1).create();
      // wait for topic creation
      TimeUnit.SECONDS.sleep(10);
      var replicaOnBroker0 =
          admin.replicas(admin.topicNames()).entrySet().stream()
              .filter(replica -> replica.getValue().get(0).broker() == 0)
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      replicaOnBroker0.forEach((tp, replica) -> Assertions.assertFalse(replica.get(0).isOffline()));
      closeBroker(0);
      assertNull(logFolders().get(0));
      assertNotNull(logFolders().get(1));
      assertNotNull(logFolders().get(2));
      var offlineReplicaOnBroker0 =
          admin.replicas(admin.topicNames()).entrySet().stream()
              .filter(replica -> replica.getValue().get(0).broker() == 0)
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      offlineReplicaOnBroker0.forEach((tp, replica) -> assertTrue(replica.get(0).isOffline()));

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    restartCluster();
  }

  @ParameterizedTest
  @ValueSource(shorts = {1, 2, 3})
  void preferredLeaderElection(short replicaSize) throws InterruptedException {
    var clusterSize = brokerIds().size();
    var topic = "preferredLeaderElection_" + Utils.randomString(6);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(30).numberOfReplicas(replicaSize).create();
      TimeUnit.SECONDS.sleep(3);

      var topicPartitions =
          IntStream.range(0, 30)
              .mapToObj(i -> new TopicPartition(topic, i))
              .collect(Collectors.toUnmodifiableSet());

      var currentLeaderMap =
          (Supplier<Map<TopicPartition, Integer>>)
              () ->
                  admin.replicas(Set.of(topic)).entrySet().stream()
                      .collect(
                          Utils.toSortedMap(
                              Map.Entry::getKey,
                              e ->
                                  e.getValue().stream()
                                      .filter(Replica::leader)
                                      .findFirst()
                                      .orElseThrow()
                                      .broker()));
      var expectedReplicaList =
          currentLeaderMap.get().entrySet().stream()
              .collect(
                  Utils.toSortedMap(
                      Map.Entry::getKey,
                      entry -> {
                        int leaderBroker = entry.getValue();
                        return List.of(
                                (leaderBroker + 2) % clusterSize,
                                leaderBroker, // original leader
                                (leaderBroker + 1) % clusterSize)
                            .subList(0, replicaSize);
                      }));
      var expectedLeaderMap =
          (Supplier<Map<TopicPartition, Integer>>)
              () ->
                  expectedReplicaList.entrySet().stream()
                      .collect(
                          Utils.toSortedMap(
                              Map.Entry::getKey,
                              e -> e.getValue().stream().findFirst().orElseThrow()));

      // change replica list
      topicPartitions.forEach(
          topicPartition ->
              admin
                  .migrator()
                  .partition(topicPartition.topic(), topicPartition.partition())
                  .moveTo(expectedReplicaList.get(topicPartition)));
      TimeUnit.SECONDS.sleep(8);

      // ReplicaMigrator#moveTo will trigger leader election if current leader being kicked out of
      // replica list. This case is always true for replica size equals to 1.
      if (replicaSize == 1) {
        // ReplicaMigrator#moveTo will trigger leader election implicitly if the original leader is
        // kicked out of the replica list. Test if ReplicaMigrator#moveTo actually trigger leader
        // election implicitly.
        Assertions.assertEquals(expectedLeaderMap.get(), currentLeaderMap.get());

        // act, the Admin#preferredLeaderElection won't throw a ElectionNotNeededException
        topicPartitions.forEach(admin::preferredLeaderElection);
        TimeUnit.SECONDS.sleep(2);

        // after election
        Assertions.assertEquals(expectedLeaderMap.get(), currentLeaderMap.get());
      } else {
        // before election
        Assertions.assertNotEquals(expectedLeaderMap.get(), currentLeaderMap.get());

        // act
        topicPartitions.forEach(admin::preferredLeaderElection);
        TimeUnit.SECONDS.sleep(2);

        // after election
        Assertions.assertEquals(expectedLeaderMap.get(), currentLeaderMap.get());
      }
    }
  }
}
