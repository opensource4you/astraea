package org.astraea.topic;

import static org.junit.jupiter.api.condition.OS.WINDOWS;

import java.io.IOException;
import java.util.Set;
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
      var argument = new ReplicaCollie.Argument();
      argument.fromBrokers = Set.of(badBroker);
      argument.toBrokers = Set.of();
      argument.brokers = bootstrapServers();
      argument.verify = verify;
      var result = ReplicaCollie.execute(topicAdmin, argument);
      var assignment = result.get(new TopicPartition(topicName, 0));
      Assertions.assertEquals(1, assignment.getKey().size());
      Assertions.assertEquals(badBroker, assignment.getKey().iterator().next());
      Assertions.assertEquals(1, assignment.getValue().size());
      Assertions.assertFalse(assignment.getValue().contains(badBroker));
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
