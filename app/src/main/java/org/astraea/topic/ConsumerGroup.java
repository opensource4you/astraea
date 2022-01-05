package org.astraea.topic;

import java.util.*;
import org.apache.kafka.common.TopicPartition;

public class ConsumerGroup {
  private final String groupId;
  private final List<Member> activeMembers;
  private final Map<TopicPartition, Long> consumeProgress;
  private final Map<Member, Set<TopicPartition>> assignment;

  public ConsumerGroup(
      String groupId,
      List<Member> activeMembers,
      Map<TopicPartition, Long> consumeProgress,
      Map<Member, Set<TopicPartition>> assignment) {
    this.groupId = Objects.requireNonNull(groupId);
    this.activeMembers = List.copyOf(activeMembers);
    this.consumeProgress = Map.copyOf(consumeProgress);
    this.assignment = Map.copyOf(assignment);
  }

  public String groupId() {
    return groupId;
  }

  public Map<Member, Set<TopicPartition>> assignment() {
    return assignment;
  }

  public Map<TopicPartition, Long> consumeProgress() {
    return consumeProgress;
  }

  public List<Member> activeMembers() {
    return activeMembers;
  }
}
