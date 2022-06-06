package org.astraea.app.web;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartition;

class ProducerHandler implements Handler {

  static final String TOPIC_KEY = "topic";
  static final String PARTITION_KEY = "partition";

  private final Admin admin;

  ProducerHandler(Admin admin) {
    this.admin = admin;
  }

  Set<TopicPartition> partitions(Map<String, String> queries) {
    if (queries.containsKey(TOPIC_KEY) && queries.containsKey(PARTITION_KEY))
      return Set.of(TopicPartition.of(queries.get(TOPIC_KEY), queries.get(PARTITION_KEY)));
    var partitions = admin.partitions();
    if (queries.containsKey(TOPIC_KEY))
      return partitions.stream()
          .filter(p -> p.topic().equals(queries.get(TOPIC_KEY)))
          .collect(Collectors.toSet());
    return partitions;
  }

  @Override
  public JsonObject get(Optional<String> target, Map<String, String> queries) {
    var topics =
        admin.producerStates(partitions(queries)).entrySet().stream()
            .map(
                e ->
                    new Partition(
                        e.getKey().topic(),
                        e.getKey().partition(),
                        e.getValue().stream()
                            .map(
                                s ->
                                    new ProducerState(
                                        s.producerId(),
                                        s.producerEpoch(),
                                        s.lastSequence(),
                                        s.lastTimestamp()))
                            .collect(Collectors.toUnmodifiableList())))
            .collect(Collectors.toUnmodifiableList());
    return new Partitions(topics);
  }

  static class ProducerState implements JsonObject {

    final long producerId;
    final int producerEpoch;
    final int lastSequence;
    final long lastTimestamp;

    ProducerState(long producerId, int producerEpoch, int lastSequence, long lastTimestamp) {
      this.producerId = producerId;
      this.producerEpoch = producerEpoch;
      this.lastSequence = lastSequence;
      this.lastTimestamp = lastTimestamp;
    }
  }

  static class Partition implements JsonObject {
    final String topic;
    final int partition;
    final List<ProducerState> states;

    Partition(String topic, int partition, List<ProducerState> states) {
      this.topic = topic;
      this.partition = partition;
      this.states = states;
    }
  }

  static class Partitions implements JsonObject {
    final List<Partition> partitions;

    Partitions(List<Partition> partitions) {
      this.partitions = partitions;
    }
  }
}
