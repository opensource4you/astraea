package org.astraea.topic;

import static org.junit.jupiter.api.condition.OS.WINDOWS;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.astraea.Utils;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;

public class ReplicaCollieTest extends RequireBrokerCluster {

  @Test
  @DisabledOnOs(WINDOWS)
  void testVerify() throws IOException, InterruptedException {
    test(true);
  }

  @Test
  @DisabledOnOs(WINDOWS)
  void testExecute() throws IOException, InterruptedException {
    test(false);
  }

  @Test
  @DisabledOnOs(WINDOWS)
  void testBrokerMigrator() throws IOException, InterruptedException {
    var topicName = "ReplicaCollieTest";
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
      topicAdmin
          .creator()
          .topic(topicName)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 1)
          .create();
      // wait for topic creation
      TimeUnit.SECONDS.sleep(5);
      var partitionReplicas = topicAdmin.replicas(Set.of(topicName));
      Assertions.assertEquals(1, partitionReplicas.size());
      var brokerSource = partitionReplicas.get(new TopicPartition(topicName, 0)).get(0).broker();
      var brokerSink =
          topicAdmin.brokerIds().stream().filter(b -> b != brokerSource).iterator().next();
      TreeMap<TopicPartition, Map.Entry<Integer, Integer>> brokerMigrate =
          new TreeMap<>(
              Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
      brokerMigrate.put(new TopicPartition(topicName, 0), Map.entry(brokerSource, brokerSink));
      Assertions.assertNotEquals(brokerSource, brokerSink);
      ReplicaCollie.brokerMigrator(brokerMigrate, topicAdmin);
      Utils.waitFor(
          () ->
              topicAdmin
                      .replicas(Set.of(topicName))
                      .get(new TopicPartition(topicName, 0))
                      .get(0)
                      .broker()
                  == brokerSink);
    }
  }

  @Test
  @DisabledOnOs(WINDOWS)
  void testPathMigrator() throws IOException, InterruptedException {
    var topicName = "ReplicaCollieTest";
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
      topicAdmin
          .creator()
          .topic(topicName)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 1)
          .create();
      // wait for topic creation
      TimeUnit.SECONDS.sleep(5);
      var partitionReplicas = topicAdmin.replicas(Set.of(topicName));
      Assertions.assertEquals(1, partitionReplicas.size());
      var brokerSource = partitionReplicas.get(new TopicPartition(topicName, 0)).get(0).broker();
      var pathSource = partitionReplicas.get(new TopicPartition(topicName, 0)).get(0).path();
      var pathSink =
          topicAdmin.brokerFolders(Set.of(brokerSource)).get(brokerSource).stream()
              .filter(p -> !p.equals(pathSource))
              .iterator()
              .next();
      TreeMap<TopicPartition, Map.Entry<String, String>> pathMigrate =
          new TreeMap<>(
              Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
      pathMigrate.put(new TopicPartition(topicName, 0), Map.entry(pathSource, pathSink));
      Assertions.assertNotEquals(pathSource, pathSink);
      ReplicaCollie.pathMigrator(pathMigrate, topicAdmin, brokerSource);
      Utils.waitFor(
          () ->
              topicAdmin
                  .replicas(Set.of(topicName))
                  .get(new TopicPartition(topicName, 0))
                  .get(0)
                  .path()
                  .equals(pathSink));
    }
  }

  private void test(boolean verify) throws IOException, InterruptedException {
    var topicName = "ReplicaCollieTest-" + verify;
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
      topicAdmin
          .creator()
          .topic(topicName)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 1)
          .create();
      // wait for topic creation
      TimeUnit.SECONDS.sleep(5);
      var partitionReplicas = topicAdmin.replicas(Set.of(topicName));
      Assertions.assertEquals(1, partitionReplicas.size());
      var replicas =
          partitionReplicas.get(new TopicPartition(topicName, 0)).stream()
              .filter(Replica::isCurrent)
              .collect(Collectors.toList());
      Assertions.assertEquals(1, replicas.size());
      var badBroker = replicas.get(0).broker();
      var badPath = replicas.get(0).path();
      var targetBroker =
          topicAdmin.brokerIds().stream()
              .filter(b -> b != badBroker)
              .collect(Collectors.toSet())
              .iterator()
              .next();
      var targetPath =
          topicAdmin.brokerFolders(Set.of(targetBroker)).get(targetBroker).iterator().next();
      var argument = new ReplicaCollie.Argument();
      argument.fromBrokers = Set.of(badBroker);
      argument.toBrokers = Set.of(targetBroker);
      argument.brokers = bootstrapServers();
      argument.topics = Set.of(topicName);
      argument.partitions = Set.of(0);
      argument.path = Set.of(targetPath);
      argument.verify = verify;
      var result = ReplicaCollie.execute(topicAdmin, argument);
      var assignment = result.get(new TopicPartition(topicName, 0));
      Assertions.assertEquals(badBroker, assignment.brokerSource);
      Assertions.assertNotEquals(badBroker, assignment.brokerSink);
      Assertions.assertEquals(badPath, assignment.pathSource);
      Assertions.assertNotEquals(badPath, assignment.pathSink);
      if (verify) {
        var currentReplicas = topicAdmin.replicas(Set.of(topicName));
        Assertions.assertEquals(partitionReplicas.size(), currentReplicas.size());
        partitionReplicas.forEach(
            (tp, rs) -> {
              var currentRs = currentReplicas.get(tp);
              Assertions.assertEquals(rs.size(), currentRs.size());
              for (var index = 0; index != rs.size(); ++index)
                Assertions.assertEquals(rs.get(index), currentRs.get(index));
            });
        Assertions.assertEquals(targetBroker, assignment.brokerSink);
        Assertions.assertEquals(targetPath, assignment.pathSink);
      } else {
        Utils.waitFor(
            () -> {
              var rs = topicAdmin.replicas(Set.of(topicName)).get(new TopicPartition(topicName, 0));
              return rs.size() == 1 && rs.stream().noneMatch(r -> r.broker() == badBroker);
            });
      }
    }
  }
}
