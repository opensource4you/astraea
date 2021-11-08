package org.astraea.topic;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicAdminArgument;

public class ReplicaFolder {

  static Map<TopicPartition, Map.Entry<Set<Integer>, Set<Integer>>> execute(
      TopicAdmin admin, Argument args) {
    var topics = (args.topics.isEmpty() ? admin.topicNames() : args.topics).iterator().next();
    var partitions = args.partitions;
    var path = args.path.iterator().next();

    var result =
        new TreeMap<TopicPartition, Map.Entry<Set<Integer>, Set<Integer>>>(
            Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));


    result.forEach(
        (tp, assignments) -> {
          // if (!args.verify) admin.reassign(tp.topic(), tp.partition(), assignments.getValue());
        });
    admin.reassignFolder(topics, partitions, path);
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
        converter = ArgumentUtil.StringSetConverter.class)
    public Set<String> topics = Collections.emptySet();

    @Parameter(
        names = {"--partitions"},
        description = "The partition that will be moved",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = ArgumentUtil.StringSetConverter.class)
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
