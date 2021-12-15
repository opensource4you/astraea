package org.astraea.topic;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicArgumentWithPropFile;

public class TopicExplorer {

  static class Result {
    final String topic;
    final int partition;
    final long earliestOffset;
    final long latestOffset;
    final List<Group> groups;
    final List<Replica> replicas;

    Result(
        String topic,
        int partition,
        long startOffset,
        long endOffset,
        List<Group> groups,
        List<Replica> replicas) {
      this.topic = topic;
      this.partition = partition;
      this.earliestOffset = startOffset;
      this.latestOffset = endOffset;
      this.groups =
          groups.stream().sorted(Comparator.comparing(Group::groupId)).collect(Collectors.toList());
      this.replicas =
          replicas.stream()
              .sorted(Comparator.comparing(Replica::broker))
              .collect(Collectors.toList());
    }

    @Override
    public String toString() {
      return "topic='"
          + topic
          + '\''
          + ", partition="
          + partition
          + ", earliestOffset="
          + earliestOffset
          + ", latestOffset="
          + latestOffset
          + ", groups="
          + groups
          + ", replicas="
          + replicas;
    }
  }

  static List<Result> execute(TopicAdmin admin, Set<String> topics) {
    var replicas = admin.replicas(topics);
    var offsets = admin.offsets(topics);
    var groups = admin.groups(topics);

    return replicas.entrySet().stream()
        .filter(e -> offsets.containsKey(e.getKey()))
        .map(
            e ->
                new Result(
                    e.getKey().topic(),
                    e.getKey().partition(),
                    offsets.get(e.getKey()).earliest(),
                    offsets.get(e.getKey()).latest(),
                    groups.getOrDefault(e.getKey(), List.of()),
                    e.getValue()))
        .sorted(
            Comparator.comparing((Result r) -> r.topic).thenComparing((Result r) -> r.partition))
        .collect(Collectors.toList());
  }

  public static void main(String[] args) throws IOException {
    var argument = ArgumentUtil.parseArgument(new Argument(), args);
    try (var admin = TopicAdmin.of(argument.props())) {
      execute(admin, argument.topics.isEmpty() ? admin.topicNames() : argument.topics)
          .forEach(System.out::println);
    }
  }

  static class Argument extends BasicArgumentWithPropFile {
    @Parameter(
        names = {"--topics"},
        description = "the topics to show all offset-related information. Empty means all topics",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = ArgumentUtil.StringSetConverter.class)
    public Set<String> topics = Collections.emptySet();
  }
}
