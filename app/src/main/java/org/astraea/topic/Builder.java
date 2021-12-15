package org.astraea.topic;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
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
    public Map<Integer, Set<String>> brokerFolders(Set<Integer> brokers) {
      return Utils.handleException(
          () ->
              admin.describeLogDirs(brokers).allDescriptions().get().entrySet().stream()
                  .collect(Collectors.toMap(Map.Entry::getKey, map -> map.getValue().keySet())));
    }

    @Override
    public Migrator migrator() {
      return new MigratorImpl(admin, this::partitions);
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
    public Map<String, TopicConfig> topics() {
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
                      new TopicConfigImpl(
                          e.getValue().entries().stream()
                              .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value)))));
    }

    @Override
    public Map<String, TopicConfig> publicTopics() {
      var topics =
          Utils.handleException(
              () -> admin.listTopics(new ListTopicsOptions().listInternal(false)).names().get());
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
                      new TopicConfigImpl(
                          e.getValue().entries().stream()
                              .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value)))));
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

      BiFunction<Integer, TopicPartition, List<Map.Entry<String, ReplicaInfo>>> findReplicas =
          (id, partition) ->
              replicaInfos.getOrDefault(id, Map.of()).entrySet().stream()
                  .flatMap(
                      entry -> {
                        var path = entry.getKey();
                        var replicas = entry.getValue().replicaInfos();
                        return replicas.entrySet().stream()
                            .filter(e -> e.getKey().equals(partition))
                            .map(e -> Map.entry(path, e.getValue()));
                      })
                  .collect(Collectors.toList());

      return Utils.handleException(
          () ->
              admin.describeTopics(topics).all().get().entrySet().stream()
                  .flatMap(
                      e ->
                          e.getValue().partitions().stream()
                              .map(
                                  topicPartitionInfo -> {
                                    var partition =
                                        new TopicPartition(
                                            e.getKey(), topicPartitionInfo.partition());
                                    return Map.entry(
                                        partition,
                                        topicPartitionInfo.replicas().stream()
                                            .flatMap(
                                                node ->
                                                    findReplicas
                                                        .apply(node.id(), partition)
                                                        .stream()
                                                        .map(
                                                            entry ->
                                                                new Replica(
                                                                    node.id(),
                                                                    entry.getValue().offsetLag(),
                                                                    entry.getValue().size(),
                                                                    topicPartitionInfo.leader().id()
                                                                        == node.id(),
                                                                    topicPartitionInfo
                                                                        .isr()
                                                                        .contains(node),
                                                                    entry.getValue().isFuture(),
                                                                    entry.getKey())))
                                            .sorted(Comparator.comparing(Replica::broker))
                                            .collect(Collectors.toList()));
                                  }))
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    @Override
    public Creator creator() {
      return new CreatorImpl(
          admin, topic -> this.replicas(Set.of(topic)), topic -> topics().get(topic));
    }
  }

  private static class TopicConfigImpl implements TopicConfig {
    private final Map<String, String> configs;

    TopicConfigImpl(Map<String, String> configs) {
      this.configs = Collections.unmodifiableMap(configs);
    }

    @Override
    public Optional<String> value(String key) {
      return Optional.ofNullable(configs.get(key));
    }

    @Override
    public Set<String> keys() {
      return configs.keySet();
    }

    @Override
    public Collection<String> values() {
      return configs.values();
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
      return configs.entrySet().iterator();
    }
  }

  private static class CreatorImpl implements Creator {
    private final Admin admin;
    private final Function<String, Map<TopicPartition, List<Replica>>> replicasGetter;
    private final Function<String, TopicConfig> configsGetter;
    private String topic;
    private int numberOfPartitions = 1;
    private short numberOfReplicas = 1;
    private final Map<String, String> configs = new HashMap<>();

    CreatorImpl(
        Admin admin,
        Function<String, Map<TopicPartition, List<Replica>>> replicasGetter,
        Function<String, TopicConfig> configsGetter) {
      this.admin = admin;
      this.replicasGetter = replicasGetter;
      this.configsGetter = configsGetter;
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
      if (Utils.handleException(() -> admin.listTopics().names().get()).contains(topic)) {
        var partitionReplicas = replicasGetter.apply(topic);
        partitionReplicas.forEach(
            (tp, replicas) -> {
              if (replicas.size() != numberOfReplicas)
                throw new IllegalArgumentException(
                    topic
                        + " is existent but its replicas: "
                        + replicas.size()
                        + " is not equal to expected: "
                        + numberOfReplicas);
            });
        var result =
            Utils.handleException(() -> admin.describeTopics(Set.of(topic)).all().get().get(topic));
        if (result.partitions().size() != numberOfPartitions)
          throw new IllegalArgumentException(
              topic
                  + " is existent but its partitions: "
                  + result.partitions().size()
                  + " is not equal to expected: "
                  + numberOfReplicas);

        var actualConfigs = configsGetter.apply(topic);
        this.configs.forEach(
            (key, value) -> {
              if (actualConfigs.value(key).filter(actual -> actual.equals(value)).isEmpty())
                throw new IllegalArgumentException(
                    topic
                        + " is existent but its config: <"
                        + key
                        + ", "
                        + actualConfigs.value(key)
                        + "> is not equal to expected: "
                        + key
                        + ", "
                        + value);
            });

        // ok, the existent topic is totally equal to what we want to create.
        return;
      }

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

  private static class MigratorImpl implements Migrator {
    private final Admin admin;
    private final Function<Set<String>, Set<TopicPartition>> partitionGetter;
    private final Set<TopicPartition> partitions = new HashSet<>();

    MigratorImpl(Admin admin, Function<Set<String>, Set<TopicPartition>> partitionGetter) {
      this.admin = admin;
      this.partitionGetter = partitionGetter;
    }

    @Override
    public Migrator topic(String topic) {
      partitions.addAll(partitionGetter.apply(Set.of(topic)));
      return this;
    }

    @Override
    public Migrator partition(String topic, int partition) {
      partitions.add(new TopicPartition(topic, partition));
      return this;
    }

    @Override
    public void moveTo(Map<Integer, String> brokerFolders) {
      Utils.handleException(
          () ->
              admin.alterReplicaLogDirs(
                  brokerFolders.entrySet().stream()
                      .collect(
                          Collectors.toMap(
                              x ->
                                  new TopicPartitionReplica(
                                      partitions.iterator().next().topic(),
                                      partitions.iterator().next().partition(),
                                      x.getKey()),
                              Map.Entry::getValue))));
    }

    @Override
    public void moveTo(Set<Integer> brokers) {
      Utils.handleException(
          () ->
              admin
                  .alterPartitionReassignments(
                      partitions.stream()
                          .collect(
                              Collectors.toMap(
                                  Function.identity(),
                                  ignore ->
                                      Optional.of(
                                          new NewPartitionReassignment(new ArrayList<>(brokers))))))
                  .all()
                  .get());
    }
  }
}
