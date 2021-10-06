package org.astraea.offset;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.astraea.argument.ArgumentUtil;
import org.astraea.topic.TopicAdmin;

public class OffsetExplorer {

  static class Result {
    final String topic;
    final int partition;
    final long startOffset;
    final long endOffset;
    final List<TopicAdmin.Group> groups;
    final List<TopicAdmin.Replica> replicas;

    @Override
    public String toString() {
      return "topic='"
          + topic
          + '\''
          + ", partition="
          + partition
          + ", startOffset="
          + startOffset
          + ", endOffset="
          + endOffset
          + ", groups="
          + groups
          + ", replicas="
          + replicas;
    }

    Result(
        String topic,
        int partition,
        long startOffset,
        long endOffset,
        List<TopicAdmin.Group> groups,
        List<TopicAdmin.Replica> replicas) {
      this.topic = topic;
      this.partition = partition;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
      this.groups = groups;
      this.replicas = replicas;
    }
  }

  static List<Result> execute(TopicAdmin admin, Set<String> topics) {
    var replicas = admin.replicas(topics);
    var offsets = admin.offset(topics);
    var groups = admin.groups(topics);

    return replicas.entrySet().stream()
        .filter(e -> offsets.containsKey(e.getKey()))
        .map(
            e ->
                new Result(
                    e.getKey().topic(),
                    e.getKey().partition(),
                    offsets.get(e.getKey()).earliest,
                    offsets.get(e.getKey()).latest,
                    groups.getOrDefault(e.getKey(), List.of()),
                    e.getValue()))
        .sorted(
            Comparator.comparing((Result r) -> r.topic).thenComparing((Result r) -> r.partition))
        .collect(Collectors.toList());
  }

  public static void main(String[] args) throws IOException {
    var argument = ArgumentUtil.parseArgument(new OffsetExplorerArgument(), args);
    try (var admin =
        TopicAdmin.of(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, argument.brokers))) {
      execute(admin, argument.topics.isEmpty() ? admin.topics() : argument.topics)
          .forEach(System.out::println);
    }
  }
}
