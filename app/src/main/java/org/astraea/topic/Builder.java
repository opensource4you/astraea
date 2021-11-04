package org.astraea.topic;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.astraea.Utils;

public class Builder {

  private final Map<String, Object> configs = new HashMap<>();

  Builder() {}

  public Builder brokers(String brokers) {
    this.configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(brokers));
    return this;
  }

  public Builder configs(Map<String, Object> configs) {
    this.configs.putAll(configs);
    return this;
  }

  public TopicAdmin build() {
    return new TopicAdminImpl(Admin.create(configs));
  }

  private static class TopicAdminImpl implements TopicAdmin {
    private final Admin admin;

    TopicAdminImpl(Admin admin) {
      this.admin = Objects.requireNonNull(admin);
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
    public Map<String, Map<String, String>> topics() {
      var topics =
          Utils.handleException(
              () -> admin.listTopics(new ListTopicsOptions().listInternal(true)).names().get());
      return Utils.handleException(
              () ->
                  admin
                      .describeConfigs(
                          topics.stream()
                              .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
                              .collect(Collectors.toList()))
                      .all()
                      .get())
          .entrySet()
          .stream()
          .collect(
              Collectors.toMap(
                  e -> e.getKey().name(),
                  e ->
                      e.getValue().entries().stream()
                          .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value))));
    }

    @Override
    public Map<TopicPartition, Offset> offsets(Set<String> topics) {
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
      var replicaInfos =
          Utils.handleException(() -> admin.describeLogDirs(brokerIds()).allDescriptions().get());
      var replicaLags =
          replicaInfos.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      e ->
                          e.getValue().values().stream()
                              .flatMap(
                                  logDirDescription ->
                                      logDirDescription.replicaInfos().entrySet().stream())
                              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));

      var replicaPaths =
          replicaInfos.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      e ->
                          e.getValue().entrySet().stream()
                              .flatMap(
                                  logDirDescription ->
                                      logDirDescription.getValue().replicaInfos().keySet().stream()
                                          .map(
                                              topicPartition ->
                                                  Map.entry(
                                                      topicPartition, logDirDescription.getKey())))
                              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));

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
                                                  node -> {
                                                    var replicaInfo =
                                                        replicaLags
                                                            .getOrDefault(node.id(), Map.of())
                                                            .get(
                                                                new TopicPartition(
                                                                    e.getKey(),
                                                                    topicPartitionInfo
                                                                        .partition()));
                                                    return new Replica(
                                                        node.id(),
                                                        replicaInfo == null
                                                            ? -1
                                                            : replicaInfo.offsetLag(),
                                                        replicaInfo == null
                                                            ? -1
                                                            : replicaInfo.size(),
                                                        topicPartitionInfo.leader().id()
                                                            == node.id(),
                                                        topicPartitionInfo.isr().contains(node),
                                                        replicaInfo != null
                                                            && replicaInfo.isFuture(),
                                                        replicaPaths
                                                            .get(node.id())
                                                            .get(
                                                                new TopicPartition(
                                                                    e.getKey(),
                                                                    topicPartitionInfo
                                                                        .partition())));
                                                  })
                                              .sorted(Comparator.comparing(Replica::broker))
                                              .collect(Collectors.toList()))))
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    @Override
    public Creator creator() {
      return new CreatorImpl(admin);
    }
  }

  private static class CreatorImpl implements Creator {
    private final Admin admin;
    private String topic;
    private int numberOfPartitions = 1;
    private short numberOfReplicas = 1;
    private final Map<String, String> configs = new HashMap<>();

    CreatorImpl(Admin admin) {
      this.admin = admin;
    }

    @Override
    public Creator topic(String topic) {
      this.topic = Objects.requireNonNull(topic);
      return this;
    }

    @Override
    public Creator numberOfPartitions(int numberOfPartitions) {
      this.numberOfPartitions = numberOfPartitions;
      return this;
    }

    @Override
    public Creator numberOfReplicas(short numberOfReplicas) {
      this.numberOfReplicas = numberOfReplicas;
      return this;
    }

    @Override
    public Creator configs(Map<String, String> configs) {
      this.configs.putAll(configs);
      return this;
    }

    @Override
    public void create() {
      Utils.handleException(
          () ->
              admin
                  .createTopics(
                      List.of(
                          new NewTopic(topic, numberOfPartitions, numberOfReplicas)
                              .configs(configs)))
                  .all()
                  .get());
    }
  }
}
