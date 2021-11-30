package org.astraea.topic;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicArgumentWithPropFile;

public class ReplicaCollie {
  static Map<TopicPartition, Map.Entry<Map.Entry<Integer, Integer>, Map.Entry<String, String>>>
      execute(TopicAdmin admin, Argument args) {
    var topics = args.topics.isEmpty() ? admin.topicNames() : args.topics;
    var allBrokers = admin.brokerIds();
    var path = args.path.isEmpty() ? null : args.path.iterator().next();
    var fromBroker = args.fromBrokers.iterator().next();
    var allPartitions =
        admin.replicas(topics).keySet().stream()
            .map(TopicPartition::partition)
            .collect(Collectors.toSet());
    if (!args.toBrokers.isEmpty() && !allBrokers.containsAll(args.toBrokers))
      throw new IllegalArgumentException(
          "those brokers: "
              + args.toBrokers.stream()
                  .filter(i -> !allBrokers.contains(i))
                  .map(String::valueOf)
                  .collect(Collectors.joining(","))
              + " are nonexistent");
    if (!args.partitions.isEmpty() && !allPartitions.containsAll(args.partitions))
      throw new IllegalArgumentException(
          "Topic "
              + topics.iterator().next()
              + " does not exist partition: "
              + args.partitions.toString());
    var targetBrokers = args.toBrokers.isEmpty() ? args.fromBrokers : args.toBrokers;
    var brokerMigratorResult =
        new TreeMap<TopicPartition, Map.Entry<Integer, Integer>>(
            Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
    var partitionMigratorResult =
        new TreeMap<TopicPartition, Map.Entry<String, String>>(
            Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
    var result =
        new TreeMap<
            TopicPartition, Map.Entry<Map.Entry<Integer, Integer>, Map.Entry<String, String>>>(
            Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
    admin.replicas(topics).entrySet().stream()
        .filter(
            tp -> args.partitions.contains(tp.getKey().partition()) || args.partitions.isEmpty())
        .collect(Collectors.toSet())
        .forEach(
            (tp) -> {
              var currentBrokers =
                  tp.getValue().stream().map(Replica::broker).collect(Collectors.toSet());
              var keptBrokers =
                  currentBrokers.stream()
                      .filter(i -> !args.fromBrokers.contains(i))
                      .collect(Collectors.toSet());
              var numberOfMigratedReplicas = currentBrokers.size() - keptBrokers.size();
              if (numberOfMigratedReplicas > 0 && !args.toBrokers.isEmpty()) {
                var availableBrokers =
                    targetBrokers.stream()
                        .filter(i -> !keptBrokers.contains(i) && !args.fromBrokers.contains(i))
                        .collect(Collectors.toSet());
                brokerMigratorResult.put(
                    tp.getKey(),
                    Map.entry(currentBrokers.iterator().next(), targetBrokers.iterator().next()));
              } else {
                brokerMigratorResult.put(
                    tp.getKey(),
                    Map.entry(currentBrokers.iterator().next(), currentBrokers.iterator().next()));
              }
            });

    if (!brokerMigratorResult.isEmpty()
        && !admin.brokerFolders(allBrokers).get(targetBrokers.iterator().next()).contains(path)
        && path != null)
      throw new IllegalArgumentException(
          "path: " + path + " is not in broker" + args.toBrokers.iterator().next());
    admin.replicas(topics).entrySet().stream()
        .filter(
            t ->
                t.getValue().get(0).broker() == fromBroker
                    && t.getKey().topic().equals(topics.iterator().next())
                    && args.partitions.contains(t.getKey().partition()))
        .collect(Collectors.toList())
        .forEach(
            (tp) -> {
              var currentPath = Set.of(tp.getValue().get(0).path());
              if (!args.path.isEmpty()) {
                if (!currentPath.iterator().next().equals(path))
                  partitionMigratorResult.put(
                      tp.getKey(), Map.entry(currentPath.iterator().next(), path));
              } else {
                partitionMigratorResult.put(
                    tp.getKey(), Map.entry(currentPath.iterator().next(), "nah"));
              }
            });
    brokerMigratorResult.forEach(
        (tp, assignments) -> {
          if (!assignments.getKey().equals(assignments.getValue())) {
            if (!args.verify)
              admin
                  .migrator()
                  .partition(tp.topic(), tp.partition())
                  .moveTo(Set.of(assignments.getValue()));
            if (!partitionMigratorResult.isEmpty())
              result.put(
                  tp,
                  Map.entry(
                      Map.entry(assignments.getKey(), assignments.getValue()),
                      Map.entry(
                          partitionMigratorResult.get(tp).getKey(),
                          partitionMigratorResult.get(tp).getValue())));
          }
        });
    partitionMigratorResult.forEach(
        (tp, assignments) -> {
          if (!args.verify) {
            admin
                .migrator()
                .partition(tp.topic(), tp.partition())
                .moveTo(Map.of(targetBrokers.iterator().next(), assignments.getValue()));
          }
          if (!result.containsKey(tp))
            result.put(
                tp,
                Map.entry(
                    Map.entry(
                        brokerMigratorResult.get(tp).getKey(),
                        brokerMigratorResult.get(tp).getValue()),
                    Map.entry(assignments.getKey(), assignments.getValue())));
          if (assignments.getValue().equals("nah")) {
            result.put(
                tp,
                Map.entry(
                    Map.entry(
                        brokerMigratorResult.get(tp).getKey(),
                        brokerMigratorResult.get(tp).getValue()),
                    Map.entry(assignments.getKey(), admin.replicas(topics).get(tp).get(0).path())));
          }
        });
    return result;
  }

  public static void main(String[] args) throws IOException {
    var argument = ArgumentUtil.parseArgument(new Argument(), args);
    try (var admin = TopicAdmin.of(argument.props())) {
      execute(admin, argument)
          .forEach(
              (tp, assignments) ->
                  System.out.println(
                      "topic: "
                          + tp.topic()
                          + ", partition: "
                          + tp.partition()
                          + " before: "
                          + assignments.getKey().getKey()
                          + ","
                          + assignments.getValue().getKey()
                          + " after: "
                          + assignments.getKey().getValue()
                          + ","
                          + assignments.getValue().getValue()));
    }
  }

  static class Argument extends BasicArgumentWithPropFile {
    @Parameter(
        names = {"--topics"},
        description = "Those topics' partitions will get reassigned. Empty means all topics",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = ArgumentUtil.StringSetConverter.class,
        required = true)
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
        names = {"--partitions"},
        description = "A partition that will be moved",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = ArgumentUtil.IntegerSetConverter.class)
    Set<Integer> partitions = Collections.emptySet();

    @Parameter(
        names = {"--path"},
        description = "The partition that will be moved to",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = ArgumentUtil.StringSetConverter.class)
    Set<String> path = Collections.emptySet();

    @Parameter(
        names = {"--verify"},
        description =
            "True if you just want to see the new assignment instead of executing the plan",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = ArgumentUtil.BooleanConverter.class)
    boolean verify = false;
  }
}
