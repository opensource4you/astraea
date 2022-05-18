package org.astraea.web;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.astraea.admin.Admin;
import org.astraea.admin.Config;

class TopicHandler implements Handler {

  static final String TOPIC_NAME_KEY = "name";
  static final String NUMBER_OF_PARTITIONS_KEY = "partitions";
  static final String NUMBER_OF_REPLICAS_KEY = "replicas";

  private final Admin admin;

  TopicHandler(Admin admin) {
    this.admin = admin;
  }

  Set<String> topicNames(Optional<String> target) {
    return Handler.compare(admin.topicNames(), target);
  }

  @Override
  public JsonObject get(Optional<String> target, Map<String, String> queries) {
    return get(topicNames(target));
  }

  private JsonObject get(Set<String> topicNames) {
    var topics = admin.topics(topicNames);
    var replicas = admin.replicas(topics.keySet());
    var partitions =
        admin.offsets(topics.keySet()).entrySet().stream()
            .collect(
                Collectors.groupingBy(
                    e -> e.getKey().topic(),
                    Collectors.mapping(
                        e ->
                            new Partition(
                                e.getKey().partition(),
                                e.getValue().earliest(),
                                e.getValue().latest(),
                                replicas.get(e.getKey()).stream()
                                    .map(Replica::new)
                                    .collect(Collectors.toUnmodifiableList())),
                        Collectors.toList())));

    var topicInfos =
        topics.entrySet().stream()
            .map(p -> new TopicInfo(p.getKey(), partitions.get(p.getKey()), p.getValue()))
            .collect(Collectors.toUnmodifiableList());

    if (topicNames.size() == 1 && topicInfos.size() == 1) return topicInfos.get(0);
    return new Topics(topicInfos);
  }

  static Map<String, String> remainingConfigs(PostRequest request) {
    var configs = new HashMap<>(request.raw());
    configs.remove(TOPIC_NAME_KEY);
    configs.remove(NUMBER_OF_PARTITIONS_KEY);
    configs.remove(NUMBER_OF_REPLICAS_KEY);
    return configs;
  }

  @Override
  public JsonObject post(PostRequest request) {
    admin
        .creator()
        .topic(request.value(TOPIC_NAME_KEY))
        .numberOfPartitions(request.intValue(NUMBER_OF_PARTITIONS_KEY, 1))
        .numberOfReplicas(request.shortValue(NUMBER_OF_REPLICAS_KEY, (short) 1))
        .configs(remainingConfigs(request))
        .create();
    if (admin.topicNames().contains(request.value(TOPIC_NAME_KEY))) {
      try {
        // if the topic creation is synced, we return the details.
        return get(Set.of(request.value(TOPIC_NAME_KEY)));
      } catch (UnknownTopicOrPartitionException e) {
        // swallow
      }
    }
    // Otherwise, return only name
    return new TopicInfo(request.value(TOPIC_NAME_KEY), List.of(), Map.of());
  }

  static class Topics implements JsonObject {
    final Collection<TopicInfo> topics;

    private Topics(Collection<TopicInfo> topics) {
      this.topics = topics;
    }
  }

  static class TopicInfo implements JsonObject {
    final String name;
    final List<Partition> partitions;
    final Map<String, String> configs;

    private TopicInfo(String name, List<Partition> partitions, Config configs) {
      this(
          name,
          partitions,
          StreamSupport.stream(configs.spliterator(), false)
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private TopicInfo(String name, List<Partition> partitions, Map<String, String> configs) {
      this.name = name;
      this.partitions = partitions;
      this.configs = configs;
    }
  }

  static class Partition implements JsonObject {
    final int id;
    final long earliest;
    final long latest;
    final List<Replica> replicas;

    Partition(int id, long earliest, long latest, List<Replica> replicas) {
      this.id = id;
      this.earliest = earliest;
      this.latest = latest;
      this.replicas = replicas;
    }
  }

  static class Replica implements JsonObject {
    final int broker;
    final long lag;
    final long size;
    final boolean leader;
    final boolean inSync;
    final boolean isFuture;
    final String path;

    Replica(org.astraea.admin.Replica replica) {
      this(
          replica.broker(),
          replica.lag(),
          replica.size(),
          replica.leader(),
          replica.inSync(),
          replica.isFuture(),
          replica.path());
    }

    Replica(
        int broker,
        long lag,
        long size,
        boolean leader,
        boolean inSync,
        boolean isFuture,
        String path) {
      this.broker = broker;
      this.lag = lag;
      this.size = size;
      this.leader = leader;
      this.inSync = inSync;
      this.isFuture = isFuture;
      this.path = path;
    }
  }
}
