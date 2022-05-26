package org.astraea.web;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.astraea.admin.Admin;
import org.astraea.admin.Member;
import org.astraea.admin.ProducerState;

class PipelineHandler implements Handler {

  static final String ACTIVE_KEY = "active";

  private final Admin admin;

  PipelineHandler(Admin admin) {
    this.admin = admin;
  }

  @Override
  public JsonObject get(Optional<String> target, Map<String, String> queries) {
    var tps =
        topicPartitions(admin).stream()
            .filter(filter(queries))
            .collect(Collectors.toUnmodifiableList());
    return new TopicPartitions(tps);
  }

  static Predicate<TopicPartition> filter(Map<String, String> queries) {
    var flag = queries.get(ACTIVE_KEY);
    // remove the topic-partitions having no producers/consumers
    if (flag != null && flag.equalsIgnoreCase("true"))
      return tp -> !tp.to.isEmpty() || !tp.from.isEmpty();
    // remove the topic-partitions having producers/consumers
    if (flag != null && flag.equalsIgnoreCase("false"))
      return tp -> tp.to.isEmpty() && tp.from.isEmpty();
    return tp -> true;
  }

  static Collection<TopicPartition> topicPartitions(Admin admin) {
    var result =
        admin.partitions().stream()
            .collect(
                Collectors.toMap(
                    Function.identity(), tp -> new TopicPartition(tp.topic(), tp.partition())));
    admin
        .consumerGroups()
        .values()
        .forEach(
            cg ->
                cg.assignment()
                    .forEach(
                        (m, tps) ->
                            tps.stream()
                                // it could return new topic partition, so we have to remove them.
                                .filter(result::containsKey)
                                .forEach(tp -> result.get(tp).to.add(new Consumer(m)))));
    admin
        .producerStates(result.keySet())
        .forEach((tp, p) -> p.forEach(s -> result.get(tp).from.add(new Producer(s))));
    return result.values().stream()
        .sorted(
            Comparator.comparing((TopicPartition tp) -> tp.topic).thenComparing(tp -> tp.partition))
        .collect(Collectors.toUnmodifiableList());
  }

  static class Producer implements JsonObject {
    final long producerId;
    final int producerEpoch;
    final int lastSequence;
    final long lastTimestamp;

    Producer(ProducerState state) {
      this.producerId = state.producerId();
      this.producerEpoch = state.producerEpoch();
      this.lastSequence = state.lastSequence();
      this.lastTimestamp = state.lastTimestamp();
    }
  }

  static class Consumer implements JsonObject {
    final String groupId;
    final String memberId;
    final String groupInstanceId;
    final String clientId;
    final String host;

    Consumer(Member member) {
      this.groupId = member.groupId();
      this.memberId = member.memberId();
      this.groupInstanceId = member.groupInstanceId().orElse(null);
      this.clientId = member.clientId();
      this.host = member.host();
    }
  }

  static class TopicPartition implements JsonObject {
    final String topic;
    final int partition;
    final Collection<Producer> from = new ArrayList<>();
    final Collection<Consumer> to = new ArrayList<>();

    TopicPartition(String topic, int partition) {
      this.topic = topic;
      this.partition = partition;
    }
  }

  static class TopicPartitions implements JsonObject {
    final Collection<TopicPartition> topicPartitions;

    TopicPartitions(Collection<TopicPartition> topicPartitions) {
      this.topicPartitions = topicPartitions;
    }
  }
}
