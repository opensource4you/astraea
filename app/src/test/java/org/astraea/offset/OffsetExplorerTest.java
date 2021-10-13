package org.astraea.offset;

import java.util.*;
import org.apache.kafka.common.TopicPartition;
import org.astraea.topic.TopicAdmin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OffsetExplorerTest {

  @Test
  void test() {

    var topicName = "topic";
    var partition = 1000;
    var topicPartition = new TopicPartition(topicName, partition);
    var latestOffset = 1000L;
    var earliestOffset = 100L;
    var groupId = "thisIsId";
    var groupOffset = 100;
    var brokerId = 10;
    var lag = 2;
    var leader = true;
    var inSync = true;
    try (var admin =
        new TopicAdmin() {
          @Override
          public Set<String> topics() {
            throw new UnsupportedOperationException();
          }

          @Override
          public Map<TopicPartition, Offset> offset(Set<String> topics) {
            return Map.of(topicPartition, new Offset(earliestOffset, latestOffset));
          }

          @Override
          public Map<TopicPartition, List<Group>> groups(Set<String> topics) {
            return Map.of(
                topicPartition,
                List.of(new Group(groupId, OptionalLong.of(groupOffset), List.of())));
          }

          @Override
          public Map<TopicPartition, List<Replica>> replicas(Set<String> topics) {
            return Map.of(topicPartition, List.of(new Replica(brokerId, lag, leader, inSync)));
          }

          @Override
          public Set<Integer> brokerIds() {
            throw new UnsupportedOperationException();
          }

          @Override
          public void reassign(String topicName, int partition, Set<Integer> brokers) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void close() {}
        }) {
      var result = OffsetExplorer.execute(admin, Set.of(topicName));
      Assertions.assertEquals(1, result.size());
      Assertions.assertEquals(topicName, result.get(0).topic);
      Assertions.assertEquals(partition, result.get(0).partition);
      Assertions.assertEquals(earliestOffset, result.get(0).earliestOffset);
      Assertions.assertEquals(latestOffset, result.get(0).latestOffset);
      Assertions.assertEquals(1, result.get(0).groups.size());
      Assertions.assertEquals(groupId, result.get(0).groups.get(0).groupId);
      Assertions.assertEquals(groupOffset, result.get(0).groups.get(0).offset.getAsLong());
      Assertions.assertEquals(1, result.get(0).replicas.size());
      Assertions.assertEquals(brokerId, result.get(0).replicas.get(0).broker);
      Assertions.assertEquals(lag, result.get(0).replicas.get(0).lag);
      Assertions.assertEquals(leader, result.get(0).replicas.get(0).leader);
      Assertions.assertEquals(inSync, result.get(0).replicas.get(0).inSync);
    }
  }
}
