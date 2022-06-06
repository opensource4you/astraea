package org.astraea.app.admin;

import static org.junit.jupiter.api.condition.OS.WINDOWS;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.astraea.app.argument.Argument;
import org.astraea.app.common.Utils;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;

public class ReplicaCollieTest extends RequireBrokerCluster {

  @Test
  @DisabledOnOs(WINDOWS)
  void testVerify() throws InterruptedException {
    test(true);
  }

  @Test
  @DisabledOnOs(WINDOWS)
  void testExecute() throws InterruptedException {
    test(false);
  }

  @Test
  @DisabledOnOs(WINDOWS)
  void testBrokerMigrator() throws InterruptedException {
    var topicName = "ReplicaCollieTest-Broker";
    try (var topicAdmin = Admin.of(bootstrapServers())) {
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
              .collect(Collectors.toList());
      var brokerSink =
          topicAdmin.brokerIds().stream().filter(b -> !brokerSource.contains(b)).iterator().next();
      var brokerMigrate = new TreeMap<TopicPartition, Map.Entry<List<Integer>, List<Integer>>>();
      brokerMigrate.put(
          new TopicPartition(topicName, 0), Map.entry(brokerSource, List.of(brokerSink)));
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
  void testPathMigrator() throws InterruptedException {
    var topicName = "ReplicaCollieTest-Path";
    try (var topicAdmin = Admin.of(bootstrapServers())) {
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
      var pathMigrate = new TreeMap<TopicPartition, Map.Entry<Set<String>, Set<String>>>();
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

  private void test(boolean verify) throws InterruptedException {
    var topicName = "ReplicaCollieTest-" + verify;
    try (var topicAdmin = Admin.of(bootstrapServers())) {
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
      var badBroker = replicas.stream().map(Replica::broker).collect(Collectors.toList());
      var targetBroker =
          topicAdmin.brokerIds().stream()
              .filter(b -> !badBroker.contains(b))
              .collect(Collectors.toList());

      var argument =
          Argument.parse(
              new ReplicaCollie.Argument(),
              new String[] {
                "--from",
                badBroker.stream().map(String::valueOf).collect(Collectors.joining(",")),
                "--to",
                targetBroker.subList(0, 1).stream()
                    .map(String::valueOf)
                    .collect(Collectors.joining(",")),
                "--bootstrap.servers",
                bootstrapServers(),
                "--topics",
                topicName,
                "--partitions",
                "0",
                verify ? "--verify" : ""
              });
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
        Assertions.assertEquals(argument.toBrokers, assignment.brokerSink);
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
