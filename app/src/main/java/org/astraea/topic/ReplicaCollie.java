package org.astraea.topic;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.TopicPartition;
import org.astraea.argument.ArgumentUtil;

public class ReplicaCollie {

  static Map<TopicPartition, Map.Entry<Set<Integer>, Set<Integer>>> execute(
      TopicAdmin admin, Argument args) {
    var topics = args.topics.isEmpty() ? admin.topics() : args.topics;
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
    var result = new HashMap<TopicPartition, Map.Entry<Set<Integer>, Set<Integer>>>();
    admin
        .replicas(topics)
        .forEach(
            (tp, replicas) -> {
              var currentBrokers = replicas.stream().map(r -> r.broker).collect(Collectors.toSet());
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
    var arguments = ArgumentUtil.parseArgument(new Argument(), args);
    try (var admin =
        TopicAdmin.of(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, arguments.brokers))) {
      execute(admin, arguments)
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

  static class Argument {
    @Parameter(
        names = {"--bootstrap.servers"},
        description = "String: server to connect to",
        validateWith = ArgumentUtil.NotEmptyString.class,
        required = true)
    String brokers;

    @Parameter(
        names = {"--topics"},
        description = "Those topics' partitions will get reassigned",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = ArgumentUtil.StringSetConverter.class)
    Set<String> topics = Collections.emptySet();

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
