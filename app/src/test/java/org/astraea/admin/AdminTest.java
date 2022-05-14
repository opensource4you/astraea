package org.astraea.admin;

import static org.junit.jupiter.api.condition.OS.WINDOWS;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.config.TopicConfig;
import org.astraea.Utils;
import org.astraea.consumer.Consumer;
import org.astraea.consumer.Deserializer;
import org.astraea.producer.Producer;
import org.astraea.producer.Serializer;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;

public class AdminTest extends RequireBrokerCluster {

  @Test
  void testCreator() {
    var topicName = "testCreator";
    try (var topicAdmin = Admin.of(bootstrapServers())) {
      topicAdmin
          .creator()
          .topic(topicName)
          .configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"))
          .create();
      Utils.waitFor(
          () ->
              topicAdmin
                  .topics()
                  .get(topicName)
                  .value(TopicConfig.COMPRESSION_TYPE_CONFIG)
                  .filter(value -> value.equals("lz4"))
                  .isPresent());

      var config = topicAdmin.topics().get(topicName);
      Assertions.assertEquals(
          config.keys().size(), (int) StreamSupport.stream(config.spliterator(), false).count());
      config.keys().forEach(key -> Assertions.assertTrue(config.value(key).isPresent()));
      Assertions.assertTrue(config.values().contains("lz4"));
    }
  }

  @Test
  void testCreateTopicRepeatedly() {
    var topicName = "testCreateTopicRepeatedly";
    try (var topicAdmin = Admin.of(bootstrapServers())) {
      Runnable createTopic =
          () ->
              topicAdmin
                  .creator()
                  .configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"))
                  .numberOfReplicas((short) 1)
                  .numberOfPartitions(3)
                  .topic(topicName)
                  .create();

      createTopic.run();
      Utils.waitFor(() -> topicAdmin.topics().containsKey(topicName));
      IntStream.range(0, 10).forEach(i -> createTopic.run());

      // changing number of partitions can producer error
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> topicAdmin.creator().numberOfPartitions(1).topic(topicName).create());

      // changing number of replicas can producer error
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> topicAdmin.creator().numberOfReplicas((short) 2).topic(topicName).create());

      // changing config can producer error
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              topicAdmin
                  .creator()
                  .configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip"))
                  .topic(topicName)
                  .create());
    }
  }

  @Test
  void testPartitions() throws InterruptedException {
    var topicName = "testPartitions";
    try (var topicAdmin = Admin.of(bootstrapServers())) {
      topicAdmin.creator().topic(topicName).numberOfPartitions(3).create();
      // wait for syncing topic creation
      TimeUnit.SECONDS.sleep(5);
      Assertions.assertTrue(topicAdmin.topicNames().contains(topicName));
      var partitions = topicAdmin.replicas(Set.of(topicName));
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
    try (var topicAdmin = Admin.of(bootstrapServers())) {
      topicAdmin.creator().topic(topicName).numberOfPartitions(3).create();
      // wait for syncing topic creation
      TimeUnit.SECONDS.sleep(5);
      var offsets = topicAdmin.offsets(Set.of(topicName));
      Assertions.assertEquals(3, offsets.size());
      offsets
          .values()
          .forEach(
              offset -> {
                Assertions.assertEquals(0, offset.earliest());
                Assertions.assertEquals(0, offset.latest());
              });
    }
  }

  @Test
  void testConsumerGroups() throws InterruptedException {
    var topicName = "testConsumerGroups-Topic";
    var consumerGroup = "testConsumerGroups-Group";
    try (var topicAdmin = Admin.of(bootstrapServers())) {
      topicAdmin.creator().topic(topicName).numberOfPartitions(3).create();
      Consumer.builder()
          .bootstrapServers(bootstrapServers())
          .topics(Set.of(topicName))
          .groupId(consumerGroup)
          .build();
      // wait for syncing topic creation
      TimeUnit.SECONDS.sleep(5);
      var consumerGroupMap = topicAdmin.consumerGroups(Set.of(consumerGroup));
      Assertions.assertEquals(1, consumerGroupMap.size());
      Assertions.assertTrue(consumerGroupMap.containsKey(consumerGroup));
      Assertions.assertEquals(consumerGroup, consumerGroupMap.get(consumerGroup).groupId());
    }
  }

  @Test
  // There is a problem when migrating the log folder under Windows because the migrated source
  // cannot be deleted, so disabled this test on Windows for now.
  @DisabledOnOs(WINDOWS)
  void testMigrateSinglePartition() throws InterruptedException {
    var topicName = "testMigrateSinglePartition";
    try (var topicAdmin = Admin.of(bootstrapServers())) {
      topicAdmin.creator().topic(topicName).numberOfPartitions(1).create();
      // wait for syncing topic creation
      TimeUnit.SECONDS.sleep(5);
      var broker = topicAdmin.brokerIds().iterator().next();
      topicAdmin.migrator().partition(topicName, 0).moveTo(List.of(broker));
      Utils.waitFor(
          () -> {
            var replicas = topicAdmin.replicas(Set.of(topicName));
            var partitionReplicas = replicas.entrySet().iterator().next().getValue();
            return replicas.size() == 1
                && partitionReplicas.size() == 1
                && partitionReplicas.get(0).broker() == broker;
          });

      var currentBroker =
          topicAdmin
              .replicas(Set.of(topicName))
              .get(new TopicPartition(topicName, 0))
              .get(0)
              .broker();
      var allPath = topicAdmin.brokerFolders(Set.of(currentBroker));
      var otherPath =
          allPath.get(currentBroker).stream()
              .filter(
                  i ->
                      !i.contains(
                          topicAdmin
                              .replicas(Set.of(topicName))
                              .get(new TopicPartition(topicName, 0))
                              .get(0)
                              .path()))
              .collect(Collectors.toSet());
      topicAdmin
          .migrator()
          .partition(topicName, 0)
          .moveTo(Map.of(currentBroker, otherPath.iterator().next()));
      Utils.waitFor(
          () -> {
            var replicas = topicAdmin.replicas(Set.of(topicName));
            var partitionReplicas = replicas.entrySet().iterator().next().getValue();
            return replicas.size() == 1
                && partitionReplicas.size() == 1
                && partitionReplicas.get(0).path().equals(otherPath.iterator().next());
          });
    }
  }

  @Test
  @DisabledOnOs(WINDOWS)
  void testChangeReplicaLeader() throws InterruptedException {
    var topicName = "testChangeReplicaLeader";
    var tp = new TopicPartition(topicName, 0);
    try (var topicAdmin = Admin.of(bootstrapServers())) {
      topicAdmin
          .creator()
          .topic(topicName)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 2)
          .create();
      // wait for syncing topic creation
      TimeUnit.SECONDS.sleep(5);
      topicAdmin.replicas(Set.of(topicName)).get(tp);
      var replicas = topicAdmin.replicas(Set.of(topicName)).get(tp);
      var oldLeader =
          replicas.stream().filter(Replica::leader).collect(Collectors.toList()).get(0).broker();
      var newLeader =
          replicas.stream().filter(b -> !b.leader()).collect(Collectors.toList()).get(0).broker();
      topicAdmin.changeReplicaLeader(Map.of(tp, newLeader));
      Assertions.assertNotEquals(oldLeader, newLeader);
      Utils.waitFor(
          () ->
              topicAdmin.replicas(Set.of(topicName)).get(tp).stream()
                  .filter(r -> r.broker() == newLeader)
                  .iterator()
                  .next()
                  .leader());
    }
  }

  @Test
  @DisabledOnOs(WINDOWS)
  void testMigrateAllPartitions() throws InterruptedException {
    var topicName = "testMigrateAllPartitions";
    try (var topicAdmin = Admin.of(bootstrapServers())) {
      topicAdmin.creator().topic(topicName).numberOfPartitions(3).create();
      // wait for syncing topic creation
      TimeUnit.SECONDS.sleep(5);
      var broker = topicAdmin.brokerIds().iterator().next();
      topicAdmin.migrator().topic(topicName).moveTo(List.of(broker));
      Utils.waitFor(
          () -> {
            var replicas = topicAdmin.replicas(Set.of(topicName));
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
    try (var topicAdmin = Admin.of(bootstrapServers());
        var producer = Producer.builder().bootstrapServers(bootstrapServers()).build()) {
      producer.sender().topic(topicName).key(new byte[100]).run().toCompletableFuture().get();
      var originSize =
          topicAdmin
              .replicas(Set.of(topicName))
              .entrySet()
              .iterator()
              .next()
              .getValue()
              .get(0)
              .size();

      // add data again
      producer.sender().topic(topicName).key(new byte[100]).run().toCompletableFuture().get();

      var newSize =
          topicAdmin
              .replicas(Set.of(topicName))
              .entrySet()
              .iterator()
              .next()
              .getValue()
              .get(0)
              .size();
      Assertions.assertTrue(newSize > originSize);
    }
  }

  @Test
  void testCompact() throws InterruptedException {
    var topicName = "testCompacted";
    try (var topicAdmin = Admin.of(bootstrapServers())) {
      topicAdmin.creator().topic(topicName).compactionMaxLag(Duration.ofSeconds(1)).create();

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
    }
  }
}
