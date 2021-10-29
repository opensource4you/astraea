package org.astraea.topic;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicAdminArgument;

public class ReplicaCollie {

  static Map<TopicPartition, Map.Entry<Set<Integer>, Set<Integer>>> execute(
      TopicAdmin admin, Argument args) {
    var topics = args.topics.isEmpty() ? admin.topicNames() : args.topics;
    var allBrokers = admin.brokerIds();
    if (!args.toBrokers.isEmpty() && !allBrokers.containsAll(args.toBrokers))
      throw new IllegalArgumentException(
          "those brokers: "
              + args.toBrokers.stream()
                  .filter(i -> !allBrokers.contains(i))
                  .map(String::valueOf)
                  .collect(Collectors.joining(","))
              + " are nonexistent");

    var targetBrokers = args.toBrokers.isEmpty() ? allBrokers : args.toBrokers;
    var result =
        new TreeMap<TopicPartition, Map.Entry<Set<Integer>, Set<Integer>>>(
            Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
    admin
        .replicas(topics)
        .forEach(
            (tp, replicas) -> {
              var currentBrokers =
                  replicas.stream().map(Replica::broker).collect(Collectors.toSet());
              var keptBrokers =
                  currentBrokers.stream()
                      .filter(i -> !args.fromBrokers.contains(i))
                      .collect(Collectors.toSet());
              var numberOfMigratedReplicas = currentBrokers.size() - keptBrokers.size();
              if (numberOfMigratedReplicas > 0) {
                var availableBrokers =
                    targetBrokers.stream()
                        .filter(i -> !keptBrokers.contains(i) && !args.fromBrokers.contains(i))
                        .collect(Collectors.toSet());
                if (availableBrokers.size() < numberOfMigratedReplicas)
                  throw new IllegalArgumentException(
                      "No enough available brokers! Available: "
                          + targetBrokers
                          + " current: "
                          + currentBrokers
                          + " removed: "
                          + args.fromBrokers);
                var finalBrokers = new HashSet<>(keptBrokers);
                finalBrokers.addAll(
                    new ArrayList<>(availableBrokers).subList(0, numberOfMigratedReplicas));
                result.put(tp, Map.entry(currentBrokers, finalBrokers));
              }
            });
    result.forEach(
        (tp, assignments) -> {
          if (!args.verify) admin.reassign(tp.topic(), tp.partition(), assignments.getValue());
        });
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
        names = {"--from"},
        description = "Those brokers won't hold any replicas of topics (defined by --topics)",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = ArgumentUtil.IntegerSetConverter.class,
        required = true)
    Set<Integer> fromBrokers;

    @Parameter(
        names = {"--to"},
        description = "The replicas of topics (defined by --topic) will be moved to those brokers",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = ArgumentUtil.IntegerSetConverter.class)
    Set<Integer> toBrokers = Collections.emptySet();

    @Parameter(
        names = {"--verify"},
        description =
            "True if you just want to see the new assignment instead of executing the plan",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = ArgumentUtil.BooleanConverter.class)
    boolean verify = false;
  }
}
