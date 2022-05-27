package org.astraea.admin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.astraea.Utils;
import org.astraea.cost.NodeInfo;

public class Builder {

  private final Map<String, Object> configs = new HashMap<>();

  Builder() {}

  public Builder bootstrapServers(String bootstrapServers) {
    this.configs.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(bootstrapServers));
    return this;
  }

  public Builder configs(Map<String, String> configs) {
    this.configs.putAll(configs);
    return this;
  }

  public Admin build() {
    return new AdminImpl(org.apache.kafka.clients.admin.Admin.create(configs));
  }

  private static class AdminImpl implements Admin {
    private final org.apache.kafka.clients.admin.Admin admin;

    AdminImpl(org.apache.kafka.clients.admin.Admin admin) {
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
    public Set<NodeInfo> nodes() {
      return Utils.handleException(
          () ->
              admin.describeCluster().nodes().get().stream()
                  .map(NodeInfo::of)
                  .collect(Collectors.toUnmodifiableSet()));
    }

    @Override
    public Map<Integer, Set<String>> brokerFolders(Set<Integer> brokers) {
      return Utils.handleException(
          () ->
              admin.describeLogDirs(brokers).allDescriptions().get().entrySet().stream()
                  .collect(Collectors.toMap(Map.Entry::getKey, map -> map.getValue().keySet())));
    }

    @Override
    public ReplicaMigrator migrator() {
      return new MigratorImpl(admin, this::partitions);
    }

    @Override
    public Map<TopicPartition, Collection<ProducerState>> producerStates(
        Set<TopicPartition> partitions) {
      return Utils.handleException(
          () ->
              admin
                  .describeProducers(
                      partitions.stream()
                          .map(TopicPartition::to)
                          .collect(Collectors.toUnmodifiableList()))
                  .all()
                  .get()
                  .entrySet()
                  .stream()
                  .filter(e -> !e.getValue().activeProducers().isEmpty())
                  .collect(
                      Collectors.toMap(
                          e -> TopicPartition.from(e.getKey()),
                          e ->
                              e.getValue().activeProducers().stream()
                                  .map(ProducerState::from)
                                  .collect(Collectors.toUnmodifiableList()))));
    }

    @Override
    public Set<String> consumerGroupIds() {
      return Utils.handleException(() -> admin.listConsumerGroups().all().get()).stream()
          .map(ConsumerGroupListing::groupId)
          .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Map<String, ConsumerGroup> consumerGroups(Set<String> consumerGroupNames) {
      return Utils.handleException(
          () -> {
            var consumerGroupDescriptions =
                admin.describeConsumerGroups(consumerGroupNames).all().get();

            var consumerGroupMetadata =
                consumerGroupNames.stream()
                    .map(x -> Map.entry(x, admin.listConsumerGroupOffsets(x)))
                    .map(
                        x ->
                            Map.entry(
                                x.getKey(),
                                Utils.handleException(
                                    () -> x.getValue().partitionsToOffsetAndMetadata().get())))
                    .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

            var createMember =
                (BiFunction<String, MemberDescription, Member>)
                    (s, x) ->
                        new Member(s, x.consumerId(), x.groupInstanceId(), x.clientId(), x.host());

            return consumerGroupNames.stream()
                .map(
                    groupId -> {
                      var members =
                          consumerGroupDescriptions.get(groupId).members().stream()
                              .map(x -> createMember.apply(groupId, x))
                              .collect(Collectors.toUnmodifiableList());
                      var consumeOffset =
                          consumerGroupMetadata.get(groupId).entrySet().stream()
                              .collect(
                                  Collectors.toUnmodifiableMap(
                                      tp -> TopicPartition.from(tp.getKey()),
                                      x -> x.getValue().offset()));
                      var assignment =
                          consumerGroupDescriptions.get(groupId).members().stream()
                              .collect(
                                  Collectors.toUnmodifiableMap(
                                      x -> createMember.apply(groupId, x),
                                      x ->
                                          x.assignment().topicPartitions().stream()
                                              .map(TopicPartition::from)
                                              .collect(Collectors.toSet())));

                      return Map.entry(
                          groupId, new ConsumerGroup(groupId, members, consumeOffset, assignment));
                    })
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
          });
    }

    private Map<TopicPartition, Long> earliestOffset(Set<TopicPartition> partitions) {

      return Utils.handleException(
          () ->
              admin
                  .listOffsets(
                      partitions.stream()
                          .collect(
                              Collectors.toMap(
                                  TopicPartition::to, e -> new OffsetSpec.EarliestSpec())))
                  .all()
                  .get()
                  .entrySet()
                  .stream()
                  .collect(
                      Collectors.toMap(
                          e -> TopicPartition.from(e.getKey()), e -> e.getValue().offset())));
    }

    private Map<TopicPartition, Long> latestOffset(Set<TopicPartition> partitions) {
      return Utils.handleException(
          () ->
              admin
                  .listOffsets(
                      partitions.stream()
                          .collect(
                              Collectors.toMap(
                                  TopicPartition::to, e -> new OffsetSpec.LatestSpec())))
                  .all()
                  .get()
                  .entrySet()
                  .stream()
                  .collect(
                      Collectors.toMap(
                          e -> TopicPartition.from(e.getKey()), e -> e.getValue().offset())));
    }

    @Override
    public Map<String, Config> topics(Set<String> topicNames) {
      return Utils.handleException(
              () ->
                  admin
                      .describeConfigs(
                          topicNames.stream()
                              .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
                              .collect(Collectors.toList()))
                      .all()
                      .get())
          .entrySet()
          .stream()
          .collect(Collectors.toMap(e -> e.getKey().name(), e -> new ConfigImpl(e.getValue())));
    }

    @Override
    public Set<String> topicNames() {
      return Utils.handleException(
          () -> admin.listTopics(new ListTopicsOptions().listInternal(true)).names().get());
    }

    @Override
    public Map<Integer, Config> brokers(Set<Integer> brokerIds) {
      return Utils.handleException(
              () ->
                  admin
                      .describeConfigs(
                          brokerIds.stream()
                              .map(
                                  id ->
                                      new ConfigResource(
                                          ConfigResource.Type.BROKER, String.valueOf(id)))
                              .collect(Collectors.toList()))
                      .all()
                      .get())
          .entrySet()
          .stream()
          .collect(
              Collectors.toMap(
                  e -> Integer.valueOf(e.getKey().name()), e -> new ConfigImpl(e.getValue())));
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

    @Override
    public Set<TopicPartition> partitions(Set<String> topics) {
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
    public Map<Integer, Set<TopicPartition>> partitions(
        Set<String> topics, Set<Integer> brokerIds) {
      return replicas(topics).entrySet().stream()
          .flatMap(
              e -> e.getValue().stream().map(replica -> Map.entry(replica.broker(), e.getKey())))
          .filter(e -> brokerIds.contains(e.getKey()))
          .collect(Collectors.groupingBy(Map.Entry::getKey))
          .entrySet()
          .stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey,
                  e -> e.getValue().stream().map(Map.Entry::getValue).collect(Collectors.toSet())));
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
                            .filter(e -> e.getKey().equals(TopicPartition.to(partition)))
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
    public TopicCreator creator() {
      return new CreatorImpl(
          admin, topic -> this.replicas(Set.of(topic)), topic -> topics().get(topic));
    }

    @Override
    public QuotaCreator quotaCreator() {
      return new QuotaImpl(admin);
    }

    @Override
    public Collection<Quota> quotas(Quota.Target target) {
      return quotas(
          ClientQuotaFilter.contains(
              List.of(ClientQuotaFilterComponent.ofEntityType(target.nameOfKafka()))));
    }

    @Override
    public Collection<Quota> quotas(Quota.Target target, String value) {
      return quotas(
          ClientQuotaFilter.contains(
              List.of(ClientQuotaFilterComponent.ofEntity(target.nameOfKafka(), value))));
    }

    @Override
    public Collection<Quota> quotas() {
      return quotas(ClientQuotaFilter.all());
    }

    private Collection<Quota> quotas(ClientQuotaFilter filter) {
      return Quota.of(
          Utils.handleException(() -> admin.describeClientQuotas(filter).entities().get()));
    }
  }

  private static class ConfigImpl implements Config {
    private final Map<String, String> configs;

    ConfigImpl(org.apache.kafka.clients.admin.Config config) {
      this(
          config.entries().stream()
              .filter(e -> e.value() != null)
              .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value)));
    }

    ConfigImpl(Map<String, String> configs) {
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

  private static class CreatorImpl implements TopicCreator {
    private final org.apache.kafka.clients.admin.Admin admin;
    private final Function<String, Map<TopicPartition, List<Replica>>> replicasGetter;
    private final Function<String, Config> configsGetter;
    private String topic;
    private int numberOfPartitions = 1;
    private short numberOfReplicas = 1;
    private final Map<String, String> configs = new HashMap<>();

    CreatorImpl(
        org.apache.kafka.clients.admin.Admin admin,
        Function<String, Map<TopicPartition, List<Replica>>> replicasGetter,
        Function<String, Config> configsGetter) {
      this.admin = admin;
      this.replicasGetter = replicasGetter;
      this.configsGetter = configsGetter;
    }

    @Override
    public TopicCreator topic(String topic) {
      this.topic = Objects.requireNonNull(topic);
      return this;
    }

    @Override
    public TopicCreator numberOfPartitions(int numberOfPartitions) {
      this.numberOfPartitions = numberOfPartitions;
      return this;
    }

    @Override
    public TopicCreator numberOfReplicas(short numberOfReplicas) {
      this.numberOfReplicas = numberOfReplicas;
      return this;
    }

    @Override
    public TopicCreator config(String key, String value) {
      this.configs.put(key, value);
      return this;
    }

    @Override
    public TopicCreator configs(Map<String, String> configs) {
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

  private static class MigratorImpl implements ReplicaMigrator {
    private final org.apache.kafka.clients.admin.Admin admin;
    private final Function<Set<String>, Set<TopicPartition>> partitionGetter;
    private final Set<TopicPartition> partitions = new HashSet<>();
    private boolean updateLeader = false;

    MigratorImpl(
        org.apache.kafka.clients.admin.Admin admin,
        Function<Set<String>, Set<TopicPartition>> partitionGetter) {
      this.admin = admin;
      this.partitionGetter = partitionGetter;
    }

    @Override
    public ReplicaMigrator topic(String topic) {
      partitions.addAll(partitionGetter.apply(Set.of(topic)));
      return this;
    }

    @Override
    public ReplicaMigrator partition(String topic, int partition) {
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
    public void moveTo(List<Integer> brokers) {
      Utils.handleException(
          () ->
              admin
                  .alterPartitionReassignments(
                      partitions.stream()
                          .collect(
                              Collectors.toMap(
                                  TopicPartition::to,
                                  ignore -> Optional.of(new NewPartitionReassignment(brokers)))))
                  .all()
                  .get());
    }

    @Override
    public void moveTo(int leader, Set<Integer> followers) {
      var all = new ArrayList<>(followers);
      all.add(0, leader);
      moveTo(all);
      // kafka produces error if re-election happens in single node
      if (!followers.isEmpty())
        Utils.handleException(
            () ->
                admin
                    .electLeaders(
                        ElectionType.PREFERRED,
                        partitions.stream().map(TopicPartition::to).collect(Collectors.toSet()))
                    .all()
                    .get());
    }
  }

  private static class QuotaImpl implements QuotaCreator {
    private final org.apache.kafka.clients.admin.Admin admin;

    QuotaImpl(org.apache.kafka.clients.admin.Admin admin) {
      this.admin = admin;
    }

    @Override
    public Ip ip(String ip) {
      return new Ip() {
        private int connectionRate = Integer.MAX_VALUE;

        @Override
        public Ip connectionRate(int value) {
          this.connectionRate = value;
          return this;
        }

        @Override
        public void create() {
          if (connectionRate == Integer.MAX_VALUE) return;
          Utils.handleException(
              () ->
                  admin
                      .alterClientQuotas(
                          List.of(
                              new ClientQuotaAlteration(
                                  new ClientQuotaEntity(Map.of(ClientQuotaEntity.IP, ip)),
                                  List.of(
                                      new ClientQuotaAlteration.Op(
                                          Quota.Limit.IP_CONNECTION_RATE.nameOfKafka(),
                                          (double) connectionRate)))))
                      .all()
                      .get());
        }
      };
    }

    @Override
    public Client clientId(String id) {
      return new Client() {
        private int produceRate = Integer.MAX_VALUE;
        private int consumeRate = Integer.MAX_VALUE;

        @Override
        public Client produceRate(int value) {
          this.produceRate = value;
          return this;
        }

        @Override
        public Client consumeRate(int value) {
          this.consumeRate = value;
          return this;
        }

        @Override
        public void create() {
          var q = new ArrayList<ClientQuotaAlteration.Op>();
          if (produceRate != Integer.MAX_VALUE)
            q.add(
                new ClientQuotaAlteration.Op(
                    Quota.Limit.PRODUCER_BYTE_RATE.nameOfKafka(), (double) produceRate));
          if (consumeRate != Integer.MAX_VALUE)
            q.add(
                new ClientQuotaAlteration.Op(
                    Quota.Limit.CONSUMER_BYTE_RATE.nameOfKafka(), (double) consumeRate));
          if (!q.isEmpty())
            Utils.handleException(
                () ->
                    admin
                        .alterClientQuotas(
                            List.of(
                                new ClientQuotaAlteration(
                                    new ClientQuotaEntity(Map.of(ClientQuotaEntity.CLIENT_ID, id)),
                                    q)))
                        .all()
                        .get());
        }
      };
    }
  }
}
