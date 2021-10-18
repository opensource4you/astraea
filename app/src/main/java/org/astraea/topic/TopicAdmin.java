package org.astraea.topic;

import java.io.Closeable;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.astraea.Utils;

public interface TopicAdmin extends Closeable {

  static TopicAdmin of(Map<String, Object> conf) {
    var admin = Admin.create(conf);
    return new TopicAdmin() {

      @Override
      public void createTopic(String topic, int numberOfPartitions) {
        var topics = topics();
        if (topics.contains(topic)) {
          var partitions = partitions(Set.of(topic));
          if (partitions.size() < numberOfPartitions) {
            Utils.handleException(
                () ->
                    admin
                        .createPartitions(
                            Map.of(topic, NewPartitions.increaseTo(numberOfPartitions)))
                        .all()
                        .get());
          }
          if (partitions.size() > numberOfPartitions) {
            throw new IllegalArgumentException(
                "Reducing the number of partitions is disallowed. Current: "
                    + partitions.size()
                    + " requested: "
                    + numberOfPartitions);
          }
        } else {
          Utils.handleException(
              () ->
                  admin
                      .createTopics(List.of(new NewTopic(topic, numberOfPartitions, (short) 1)))
                      .all()
                      .get());
        }
      }

      @Override
      public void close() {
        admin.close();
      }

      @Override
      public Set<Integer> brokerIds() {
        return Utils.handleException(
            () ->
                admin.describeCluster().nodes().get().stream()
                    .map(Node::id)
                    .collect(Collectors.toSet()));
      }

      @Override
      public void reassign(String topicName, int partition, Set<Integer> brokers) {
        Utils.handleException(
            () ->
                admin
                    .alterPartitionReassignments(
                        Map.of(
                            new TopicPartition(topicName, partition),
                            Optional.of(new NewPartitionReassignment(new ArrayList<>(brokers)))))
                    .all()
                    .get());
      }

      @Override
      public Map<TopicPartition, List<Group>> groups(Set<String> topics) {
        var groups =
            Utils.handleException(() -> admin.listConsumerGroups().valid().get()).stream()
                .map(ConsumerGroupListing::groupId)
                .collect(Collectors.toList());

        var allPartitions = partitions(topics);

        var result = new HashMap<TopicPartition, List<Group>>();
        Utils.handleException(() -> admin.describeConsumerGroups(groups).all().get())
            .forEach(
                (groupId, groupDescription) -> {
                  var partitionOffsets =
                      Utils.handleException(
                          () ->
                              admin
                                  .listConsumerGroupOffsets(groupId)
                                  .partitionsToOffsetAndMetadata()
                                  .get());

                  var partitionMembers =
                      groupDescription.members().stream()
                          .flatMap(
                              m ->
                                  m.assignment().topicPartitions().stream()
                                      .map(tp -> Map.entry(tp, m)))
                          .collect(Collectors.groupingBy(Map.Entry::getKey))
                          .entrySet()
                          .stream()
                          .collect(
                              Collectors.toMap(
                                  Map.Entry::getKey,
                                  e ->
                                      e.getValue().stream()
                                          .map(Map.Entry::getValue)
                                          .collect(Collectors.toList())));

                  allPartitions.forEach(
                      tp -> {
                        var offset =
                            partitionOffsets.containsKey(tp)
                                ? OptionalLong.of(partitionOffsets.get(tp).offset())
                                : OptionalLong.empty();
                        var members =
                            partitionMembers.getOrDefault(tp, List.of()).stream()
                                .map(
                                    m ->
                                        new Member(
                                            m.consumerId(),
                                            m.groupInstanceId(),
                                            m.clientId(),
                                            m.host()))
                                .collect(Collectors.toList());
                        // This group is related to the partition only if it has either member or
                        // offset.
                        if (offset.isPresent() || !members.isEmpty()) {
                          result
                              .computeIfAbsent(tp, ignore -> new ArrayList<>())
                              .add(new Group(groupId, offset, members));
                        }
                      });
                });

        return result;
      }

      private Map<TopicPartition, Long> earliestOffset(Set<TopicPartition> partitions) {

        return Utils.handleException(
            () ->
                admin
                    .listOffsets(
                        partitions.stream()
                            .collect(Collectors.toMap(e -> e, e -> new OffsetSpec.EarliestSpec())))
                    .all()
                    .get()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset())));
      }

      private Map<TopicPartition, Long> latestOffset(Set<TopicPartition> partitions) {
        return Utils.handleException(
            () ->
                admin
                    .listOffsets(
                        partitions.stream()
                            .collect(Collectors.toMap(e -> e, e -> new OffsetSpec.LatestSpec())))
                    .all()
                    .get()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset())));
      }

      @Override
      public Set<String> topics() {
        return Utils.handleException(
            () -> admin.listTopics(new ListTopicsOptions().listInternal(true)).names().get());
      }

      @Override
      public Map<TopicPartition, Offset> offset(Set<String> topics) {
        var partitions = partitions(topics);
        var earliest = earliestOffset(partitions);
        var latest = latestOffset(partitions);
        return earliest.entrySet().stream()
            .filter(e -> latest.containsKey(e.getKey()))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, e -> new Offset(e.getValue(), latest.get(e.getKey()))));
      }

      private Set<TopicPartition> partitions(Set<String> topics) {
        return Utils.handleException(
            () ->
                admin.describeTopics(topics).all().get().entrySet().stream()
                    .flatMap(
                        e ->
                            e.getValue().partitions().stream()
                                .map(p -> new TopicPartition(e.getKey(), p.partition())))
                    .collect(Collectors.toSet()));
      }

      @Override
      public Map<TopicPartition, List<Replica>> replicas(Set<String> topics) {
        var lags =
            Utils.handleException(
                () ->
                    admin.describeLogDirs(brokerIds()).allDescriptions().get().entrySet().stream()
                        .collect(
                            Collectors.toMap(
                                Map.Entry::getKey,
                                e ->
                                    e.getValue().values().stream()
                                        .flatMap(
                                            logDirDescription ->
                                                logDirDescription
                                                    .replicaInfos()
                                                    .entrySet()
                                                    .stream())
                                        .collect(
                                            Collectors.toMap(
                                                Map.Entry::getKey, Map.Entry::getValue)))));

        return Utils.handleException(
            () ->
                admin.describeTopics(topics).all().get().entrySet().stream()
                    .flatMap(
                        e ->
                            e.getValue().partitions().stream()
                                .map(
                                    topicPartitionInfo ->
                                        Map.entry(
                                            new TopicPartition(
                                                e.getKey(), topicPartitionInfo.partition()),
                                            topicPartitionInfo.replicas().stream()
                                                .map(
                                                    node ->
                                                        new Replica(
                                                            node.id(),
                                                            Optional.ofNullable(
                                                                    lags.getOrDefault(
                                                                            node.id(), Map.of())
                                                                        .get(
                                                                            new TopicPartition(
                                                                                e.getKey(),
                                                                                topicPartitionInfo
                                                                                    .partition())))
                                                                .map(ReplicaInfo::offsetLag)
                                                                .orElse(-1L),
                                                            topicPartitionInfo.leader().id()
                                                                == node.id(),
                                                            topicPartitionInfo
                                                                .isr()
                                                                .contains(node)))
                                                .sorted(
                                                    Comparator.comparing((Replica r) -> r.broker))
                                                .collect(Collectors.toList()))))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
      }
    };
  }

  /** @return the topics name (internal topics are excluded) */
  Set<String> topics();

  /**
   * make sure there is a topic having requested name and requested number of partitions. If the
   * topic is existent and the number of partitions is larger than requested number, it will throw
   * exception.
   *
   * @param topic topic name
   * @param numberOfPartitions expected number of partitions.
   */
  void createTopic(String topic, int numberOfPartitions);

  /**
   * @param topics topic names
   * @return the earliest offset and latest offset for specific topics
   */
  Map<TopicPartition, Offset> offset(Set<String> topics);

  /**
   * @param topics topic names
   * @return the partition having consumer group id and consumer group offset
   */
  Map<TopicPartition, List<Group>> groups(Set<String> topics);

  /**
   * @param topics topic names
   * @return the replicas of partition
   */
  Map<TopicPartition, List<Replica>> replicas(Set<String> topics);

  /** @return all brokers' ids */
  Set<Integer> brokerIds();

  /**
   * @param topicName topic name
   * @param partition partition
   * @param brokers to hold all the
   */
  void reassign(String topicName, int partition, Set<Integer> brokers);

  class Group {
    public final String groupId;
    public final OptionalLong offset;
    public final List<Member> members;

    public Group(String groupId, OptionalLong offset, List<Member> members) {
      this.groupId = groupId;
      this.offset = offset;
      this.members = members;
    }

    @Override
    public String toString() {
      return "Group{"
          + "groupId='"
          + groupId
          + '\''
          + ", offset="
          + (offset.isEmpty() ? "none" : offset.getAsLong())
          + ", members="
          + members
          + '}';
    }
  }

  class Member {
    private final String memberId;
    private final Optional<String> groupInstanceId;
    private final String clientId;
    private final String host;

    public Member(String memberId, Optional<String> groupInstanceId, String clientId, String host) {
      this.memberId = memberId;
      this.groupInstanceId = groupInstanceId;
      this.clientId = clientId;
      this.host = host;
    }

    @Override
    public String toString() {
      return "Member{"
          + "memberId='"
          + memberId
          + '\''
          + ", groupInstanceId="
          + groupInstanceId
          + ", clientId='"
          + clientId
          + '\''
          + ", host='"
          + host
          + '\''
          + '}';
    }
  }

  class Offset {
    public final long earliest;
    public final long latest;

    public Offset(long earliest, long latest) {
      this.earliest = earliest;
      this.latest = latest;
    }

    @Override
    public String toString() {
      return "Offset{" + "earliest=" + earliest + ", latest=" + latest + '}';
    }
  }

  class Replica {
    public final int broker;
    public final long lag;
    public final boolean leader;
    public final boolean inSync;

    public Replica(int broker, long lag, boolean leader, boolean inSync) {
      this.broker = broker;
      this.lag = lag;
      this.leader = leader;
      this.inSync = inSync;
    }

    @Override
    public String toString() {
      return "Replica{"
          + "broker="
          + broker
          + ", lag="
          + lag
          + ", leader="
          + leader
          + ", inSync="
          + inSync
          + '}';
    }
  }
}
