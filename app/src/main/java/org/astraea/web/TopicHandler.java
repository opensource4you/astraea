package org.astraea.web;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.astraea.topic.TopicAdmin;
import org.astraea.topic.TopicConfig;

class TopicHandler implements Handler {

  private final TopicAdmin admin;

  TopicHandler(TopicAdmin admin) {
    this.admin = admin;
  }

  @Override
  public JsonObject response(Optional<String> target, Map<String, String> queries) {
    Predicate<Map.Entry<String, ?>> topicFilter =
        e -> target.stream().allMatch(t -> t.equals(e.getKey()));
    var topics = admin.topics();
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
            .filter(topicFilter)
            .map(p -> new TopicInfo(p.getKey(), partitions.get(p.getKey()), p.getValue()))
            .collect(Collectors.toUnmodifiableList());

    if (target.isPresent() && topicInfos.size() == 1) return topicInfos.get(0);
    else if (target.isPresent())
      throw new NoSuchElementException("topic: " + target.get() + " does not exist");
    else return new Topics(topicInfos);
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

    private TopicInfo(String name, List<Partition> partitions, TopicConfig configs) {
      this.name = name;
      this.partitions = partitions;
      this.configs =
          StreamSupport.stream(configs.spliterator(), false)
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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
    private final int broker;
    private final long lag;
    private final long size;
    private final boolean leader;
    private final boolean inSync;
    private final boolean isFuture;
    private final String path;

    Replica(org.astraea.topic.Replica replica) {
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
