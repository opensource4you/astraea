package org.astraea.offset;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicAdminArgument;
import org.astraea.topic.TopicAdmin;

public class OffsetExplorer {

  static class Result {
    final String topic;
    final int partition;
    final long earliestOffset;
    final long latestOffset;
    final List<TopicAdmin.Group> groups;
    final List<TopicAdmin.Replica> replicas;

    Result(
        String topic,
        int partition,
        long startOffset,
        long endOffset,
        List<TopicAdmin.Group> groups,
        List<TopicAdmin.Replica> replicas) {
      this.topic = topic;
      this.partition = partition;
      this.earliestOffset = startOffset;
      this.latestOffset = endOffset;
      this.groups = groups;
      this.replicas = replicas;
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
    var argument = ArgumentUtil.parseArgument(new Argument(), args);
    try (var admin = TopicAdmin.of(argument.adminProps())) {
      execute(admin, argument.topics.isEmpty() ? admin.topics() : argument.topics)
          .forEach(System.out::println);
    }
  }

  static class Argument extends BasicAdminArgument {
    @Parameter(
        names = {"--topics"},
        description = "the topics to show all offset-related information. Empty means all topics",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = ArgumentUtil.StringSetConverter.class)
    public Set<String> topics = Collections.emptySet();
  }
}
