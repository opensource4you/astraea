package org.astraea.topic;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.config.TopicConfig;
import org.astraea.Utils;
import org.astraea.producer.Producer;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;

import static org.junit.jupiter.api.condition.OS.WINDOWS;

public class TopicAdminTest extends RequireBrokerCluster {

  @Test
  void testCreator() throws IOException {
    var topicName = "testCreator";
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
      topicAdmin
          .creator()
          .topic(topicName)
          .configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"))
          .create();
      Utils.waitFor(
          () ->
              topicAdmin
                  .topics()
                  .getOrDefault(topicName, Map.of())
                  .get(TopicConfig.COMPRESSION_TYPE_CONFIG)
                  .equals("lz4"));
    }
  }

  @Test
  void testCreateTopicRepeatedly() throws IOException {
    var topicName = "testCreateTopicRepeatedly";
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
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
  void testPartitions() throws IOException, InterruptedException {
    var topicName = "testPartitions";
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
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
  void testGroups() throws IOException, InterruptedException {
    var topicName = "testGroups";
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
      topicAdmin.creator().topic(topicName).numberOfPartitions(3).create();
      // wait for syncing topic creation
      TimeUnit.SECONDS.sleep(5);
      Assertions.assertTrue(topicAdmin.topicNames().contains(topicName));
      var groups = topicAdmin.groups(Set.of(topicName));
      Assertions.assertEquals(0, groups.size());
    }
  }

  @Test
  void testOffsets() throws IOException, InterruptedException {
    var topicName = "testOffsets";
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
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
  @DisabledOnOs(WINDOWS)
  void testReassign() throws IOException, InterruptedException {
    var topicName = "testReassign";
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
      topicAdmin.creator().topic(topicName).numberOfPartitions(1).create();
      // wait for syncing topic creation
      TimeUnit.SECONDS.sleep(5);
      var broker = topicAdmin.brokerIds().iterator().next();
      topicAdmin.reassign(topicName, 0, Set.of(broker));
      Utils.waitFor(
          () -> {
            var replicas = topicAdmin.replicas(Set.of(topicName));
            var partitionReplicas = replicas.entrySet().iterator().next().getValue();
            return replicas.size() == 1
                && partitionReplicas.size() == 1
                && partitionReplicas.get(0).broker() == broker;
          });
    }
  }

  @Test
  void testReplicaSize() throws IOException, ExecutionException, InterruptedException {
    var topicName = "testReplicaSize";
    try (var topicAdmin = TopicAdmin.of(bootstrapServers());
        var producer = Producer.builder().brokers(bootstrapServers()).build()) {
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
}
