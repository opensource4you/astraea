package org.astraea.topic;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReplicaCollieTest {

  private final String topicName = "ReplicaCollieTest";
  private final int partition = 0;
  private final int badBroker = 10;
  private final int goodBroker = 100;
  private final int earliestOffset = 199;
  private final int latestOffset = 299;
  private final Set<Integer> reassignment = new HashSet<>();
  private final TopicAdmin admin =
      new TopicAdmin() {
        @Override
        public Set<String> topics() {
          return Set.of(topicName);
        }

        @Override
        public Map<TopicPartition, Offset> offset(Set<String> topics) {
          throw new UnsupportedOperationException();
        }

        @Override
        public Map<TopicPartition, List<Group>> groups(Set<String> topics) {
          throw new UnsupportedOperationException();
        }

        @Override
        public Map<TopicPartition, List<Replica>> replicas(Set<String> topics) {
          return Map.of(
              new TopicPartition(topicName, partition),
              List.of(new Replica(badBroker, 0, true, true)));
        }

        @Override
        public Set<Integer> brokerIds() {
          return Set.of(goodBroker, badBroker);
        }

        @Override
        public void reassign(String topicName, int partition, Set<Integer> brokers) {
          reassignment.addAll(brokers);
        }

        @Override
        public void close() {
          throw new UnsupportedOperationException();
        }
      };

  @Test
  void testVerify() {
    test(true);
  }

  @Test
  void testExecute() {
    test(false);
  }

  private void test(boolean verify) {
    var argument = new ReplicaCollie.Argument();
    argument.fromBrokers = Set.of(badBroker);
    argument.toBrokers = Set.of();
    argument.brokers = "just unit test";
    argument.verify = verify;
    var result = ReplicaCollie.execute(admin, argument);
    Assertions.assertEquals(1, result.size());
    var assignment = result.get(new TopicPartition(topicName, partition));
    Assertions.assertEquals(1, assignment.getKey().size());
    Assertions.assertEquals(badBroker, assignment.getKey().iterator().next());
    Assertions.assertEquals(1, assignment.getValue().size());
    Assertions.assertEquals(goodBroker, assignment.getValue().iterator().next());
    if (verify) {
      Assertions.assertEquals(0, reassignment.size());
    } else {
      Assertions.assertEquals(1, reassignment.size());
      Assertions.assertEquals(goodBroker, reassignment.iterator().next());
    }
  }
}
