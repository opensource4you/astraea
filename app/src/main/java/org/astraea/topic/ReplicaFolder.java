package org.astraea.topic;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicAdminArgument;

public class ReplicaFolder {

  static Map<TopicPartition, Map.Entry<Set<String>, Set<String>>> execute(
      TopicAdmin admin, Argument args) {
    var topic = args.topics.iterator().next();
    var partitions = args.partitions.stream().map(Integer::valueOf).collect(Collectors.toSet());
    var path = args.path.iterator().next();
    var currentBroker =
        admin
            .replicas(args.topics)
            .get(new TopicPartition(topic, partitions.iterator().next()))
            .get(0)
            .broker();
    var result =
        new TreeMap<TopicPartition, Map.Entry<Set<String>, Set<String>>>(
            Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
    admin.replicas(args.topics).entrySet().stream()
        .filter(
            t -> t.getKey().topic().equals(topic) && partitions.contains(t.getKey().partition()))
        .collect(Collectors.toList())
        .forEach(
            (tp) -> {
              var currentPath =
                  tp.getValue().stream().map(Replica::path).collect(Collectors.toSet());
              if (tp.getValue().get(0).broker() == currentBroker)
                if (topic.equals(tp.getKey().topic())
                    && partitions.contains(tp.getKey().partition()))
                  if (!currentPath.equals(args.path))
                    result.put(tp.getKey(), Map.entry(currentPath, args.path));
            });
    if (!args.verify) admin.reassignFolder(currentBroker, topic, partitions, path);

    return result;
  }

  public static void main(String[] args) throws IOException {
    var argument = ArgumentUtil.parseArgument(new Argument(), args);
    try (var admin = TopicAdmin.of(argument.adminProps())) {
      execute(admin, argument)
          .forEach(
              (tp, assignments) ->
                  System.out.println(
                      "topic: "
                          + tp.topic()
                          + ", partition: "
                          + tp.partition()
                          + " before: "
                          + assignments.getKey()
                          + " after: "
                          + assignments.getValue()));
    }
  }

  static class Argument extends BasicAdminArgument {
    @Parameter(
        names = {"--topics"},
        description = "Those topics' partitions will get reassigned. Empty menas all topics",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = ArgumentUtil.StringSetConverter.class,
        required = true)
    public Set<String> topics = Collections.emptySet();

    @Parameter(
        names = {"--partitions"},
        description = "The partition that will be moved",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = ArgumentUtil.StringSetConverter.class,
        required = true)
    public Set<String> partitions = Collections.emptySet();

    @Parameter(
        names = {"--path"},
        description = "The partition that will be moved",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = ArgumentUtil.StringSetConverter.class)
    public Set<String> path = Collections.emptySet();

    @Parameter(
        names = {"--verify"},
        description =
            "True if you just want to see the new assignment instead of executing the plan",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = ArgumentUtil.BooleanConverter.class)
    boolean verify = false;
  }
}
