package org.astraea.topic;

import java.io.Closeable;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.astraea.Utils;

public interface TopicAdmin extends Closeable {

  static TopicAdmin of(Map<String, Object> conf) {
    var admin = Admin.create(conf);
    return new TopicAdmin() {

      @Override
      public void close() {
        admin.close();
      }

      @Override
      public Map<TopicPartition, List<Group>> groups(Set<String> topics) {
        return Utils.handleException(
                () ->
                    admin.listConsumerGroups().valid().get().stream()
                        .map(ConsumerGroupListing::groupId)
                        .collect(Collectors.toSet()))
            .stream()
            .flatMap(
                group ->
                    Utils.handleException(
                            () ->
                                admin
                                    .listConsumerGroupOffsets(group)
                                    .partitionsToOffsetAndMetadata()
                                    .get())
                        .entrySet()
                        .stream()
                        .filter(e -> topics.contains(e.getKey().topic()))
                        .map(e -> Map.entry(group, e)))
            .collect(Collectors.groupingBy(e -> e.getValue().getKey()))
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        e.getValue().stream()
                            .map(
                                groupOffset ->
                                    new Group(
                                        groupOffset.getKey(),
                                        groupOffset.getValue().getValue().offset()))
                            .collect(Collectors.toList())));
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
        return Utils.handleException(() -> admin.listTopics().names().get());
      }

      @Override
      public Map<TopicPartition, Offset> offset(Set<String> topics) {
        var partitions =
            Utils.handleException(
                () ->
                    admin.describeTopics(topics).all().get().entrySet().stream()
                        .flatMap(
                            e ->
                                e.getValue().partitions().stream()
                                    .map(p -> new TopicPartition(e.getKey(), p.partition())))
                        .collect(Collectors.toSet()));
        var earliest = earliestOffset(partitions);
        var latest = latestOffset(partitions);
        return earliest.entrySet().stream()
            .filter(e -> latest.containsKey(e.getKey()))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, e -> new Offset(e.getValue(), latest.get(e.getKey()))));
      }

      @Override
      public Map<TopicPartition, List<Replica>> replicas(Set<String> topics) {
        var brokers =
            Utils.handleException(
                () ->
                    admin.describeCluster().nodes().get().stream()
                        .map(Node::id)
                        .collect(Collectors.toSet()));

        var lags =
            Utils.handleException(
                () ->
                    admin.describeLogDirs(brokers).allDescriptions().get().entrySet().stream()
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

  class Group {
    public final String id;
    public final long offset;

    public Group(String id, long offset) {
      this.id = id;
      this.offset = offset;
    }

    @Override
    public String toString() {
      return "Group{" + "id='" + id + '\'' + ", offset=" + offset + '}';
    }
  }

  class Offset {
    public final long earliest;
    public final long latest;

    Offset(long earliest, long latest) {
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

    Replica(int broker, long lag, boolean leader, boolean inSync) {
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
