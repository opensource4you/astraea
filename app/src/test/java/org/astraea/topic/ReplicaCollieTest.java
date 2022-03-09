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
    var topicName = "ReplicaCollieTest-Broker";
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
      topicAdmin
          .creator()
          .topic(topicName)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 2)
          .create();
      // wait for topic creation
      TimeUnit.SECONDS.sleep(5);
      var partitionReplicas = topicAdmin.replicas(Set.of(topicName));
      Assertions.assertEquals(1, partitionReplicas.size());
      var brokerSource =
          partitionReplicas.get(new TopicPartition(topicName, 0)).stream()
              .map(Replica::broker)
              .collect(Collectors.toSet());
      var brokerSink =
          topicAdmin.brokerIds().stream().filter(b -> !brokerSource.contains(b)).iterator().next();
      TreeMap<TopicPartition, Map.Entry<Set<Integer>, Set<Integer>>> brokerMigrate =
          new TreeMap<>(
              Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
      brokerMigrate.put(
          new TopicPartition(topicName, 0), Map.entry(brokerSource, Set.of(brokerSink)));
      Assertions.assertEquals(
          topicAdmin.replicas(Set.of(topicName)).get(new TopicPartition(topicName, 0)).size(), 2);
      ReplicaCollie.brokerMigrator(brokerMigrate, topicAdmin);
      Utils.waitFor(
          () ->
              topicAdmin.replicas(Set.of(topicName)).get(new TopicPartition(topicName, 0)).size()
                      == 1
                  && topicAdmin
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
    var topicName = "ReplicaCollieTest-Path";
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
      topicAdmin
          .creator()
          .topic(topicName)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 2)
          .create();
      // wait for topic creation
      TimeUnit.SECONDS.sleep(5);
      var partitionReplicas = topicAdmin.replicas(Set.of(topicName));
      Assertions.assertEquals(1, partitionReplicas.size());
      var brokerSource = partitionReplicas.get(new TopicPartition(topicName, 0)).get(0).broker();
      var pathSource =
          partitionReplicas.get(new TopicPartition(topicName, 0)).stream()
              .map(Replica::path)
              .collect(Collectors.toSet());
      var pathSink =
          topicAdmin.brokerFolders(Set.of(brokerSource)).get(brokerSource).stream()
              .filter(p -> !pathSource.contains(p))
              .iterator()
              .next();
      TreeMap<TopicPartition, Map.Entry<Set<String>, Set<String>>> pathMigrate =
          new TreeMap<>(
              Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
      pathMigrate.put(new TopicPartition(topicName, 0), Map.entry(pathSource, Set.of(pathSink)));
      Assertions.assertFalse(pathSource.contains(pathSink));
      Assertions.assertEquals(
          topicAdmin.replicas(Set.of(topicName)).get(new TopicPartition(topicName, 0)).size(), 2);
      ReplicaCollie.pathMigrator(pathMigrate, topicAdmin, brokerSource);
      Utils.waitFor(
          () ->
              topicAdmin
                  .replicas(Set.of(topicName))
                  .get(new TopicPartition(topicName, 0))
                  .get(0)
                  .path()
                  .equals(pathSink));
      topicAdmin.replicas(Set.of(topicName)).get(new TopicPartition(topicName, 0));
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
      var badBroker = replicas.stream().map(Replica::broker).collect(Collectors.toSet());
      var targetBroker =
          topicAdmin.brokerIds().stream()
              .filter(b -> !badBroker.contains(b))
              .collect(Collectors.toSet());
      var argument = new ReplicaCollie.Argument();
      argument.fromBrokers = badBroker;
      argument.toBrokers = targetBroker;
      argument.brokers = bootstrapServers();
      argument.topics = Set.of(topicName);
      argument.partitions = Set.of(0);
      argument.verify = verify;
      var result = ReplicaCollie.execute(topicAdmin, argument);
      var assignment = result.get(new TopicPartition(topicName, 0));
      Assertions.assertEquals(badBroker, assignment.brokerSource);
      Assertions.assertNotEquals(badBroker, assignment.brokerSink);
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

      } else {
        Utils.waitFor(
            () -> {
              var rs = topicAdmin.replicas(Set.of(topicName)).get(new TopicPartition(topicName, 0));
              return rs.size() == 2
                  && rs.stream()
                      .map(Replica::broker)
                      .collect(Collectors.toSet())
                      .containsAll(argument.toBrokers);
            });
      }
    }
  }
}
