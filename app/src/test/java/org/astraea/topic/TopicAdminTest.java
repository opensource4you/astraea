package org.astraea.topic;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.astraea.Utils;
import org.astraea.producer.Producer;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TopicAdminTest extends RequireBrokerCluster {

  @Test
  void testPartitions() throws IOException, InterruptedException {
    var topicName = "testPartitions";
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
      topicAdmin.createTopic(topicName, 3);
      // wait for syncing topic creation
      TimeUnit.SECONDS.sleep(5);
      Assertions.assertTrue(topicAdmin.topics().contains(topicName));
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
                              logFolders.stream().anyMatch(replica.path::contains))));
    }
  }

  @Test
  void testGroups() throws IOException, InterruptedException {
    var topicName = "testGroups";
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
      topicAdmin.createTopic(topicName, 3);
      // wait for syncing topic creation
      TimeUnit.SECONDS.sleep(5);
      Assertions.assertTrue(topicAdmin.topics().contains(topicName));
      var groups = topicAdmin.groups(Set.of(topicName));
      Assertions.assertEquals(0, groups.size());
    }
  }

  @Test
  void testOffsets() throws IOException, InterruptedException {
    var topicName = "testOffsets";
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
      topicAdmin.createTopic(topicName, 3);
      // wait for syncing topic creation
      TimeUnit.SECONDS.sleep(5);
      var offsets = topicAdmin.offsets(Set.of(topicName));
      Assertions.assertEquals(3, offsets.size());
      offsets
          .values()
          .forEach(
              offset -> {
                Assertions.assertEquals(0, offset.earliest);
                Assertions.assertEquals(0, offset.latest);
              });
    }
  }

  @Test
  void testReassign() throws IOException, InterruptedException {
    var topicName = "testReassign";
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
      topicAdmin.createTopic(topicName, 1);
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
                && partitionReplicas.get(0).broker == broker;
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
              .size;

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
              .size;
      Assertions.assertTrue(newSize > originSize);
    }
  }
}
