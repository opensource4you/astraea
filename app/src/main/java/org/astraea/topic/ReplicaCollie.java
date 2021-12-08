package org.astraea.topic;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicArgumentWithPropFile;

public class ReplicaCollie {
  static class MigratorInfo {
    Integer brokerIdA;
    Integer brokerIdB;
    String pathA;
    String pathB;
  }

  static class Parameters {
    Set<String> topics;
    Set<Integer> partitions;
    Set<Integer> fromBrokers;
    String path;
    Set<Integer> targetBrokers;
  }

  static Parameters setParameters(TopicAdmin admin, Argument args) {
    Parameters parameters = new Parameters();
    parameters.topics = args.topics.isEmpty() ? admin.topicNames() : args.topics;
    parameters.fromBrokers = args.fromBrokers;
    parameters.partitions = args.partitions;
    parameters.path = args.path.isEmpty() ? null : args.path.iterator().next();
    if (args.partitions.isEmpty() && args.path.isEmpty() && args.toBrokers.isEmpty()) {
      parameters.targetBrokers =
          admin.brokerIds().stream()
              .filter(b -> !args.fromBrokers.contains(b))
              .collect(Collectors.toSet());
    } else {
      parameters.targetBrokers = args.toBrokers.isEmpty() ? args.fromBrokers : args.toBrokers;
    }
    return parameters;
  }

  static void illegalArgs(TopicAdmin admin, Argument args) {
    var topics = args.topics.isEmpty() ? admin.topicNames() : args.topics;
    var path = args.path.isEmpty() ? null : args.path.iterator().next();
    var targetBrokers = args.toBrokers.isEmpty() ? args.fromBrokers : args.toBrokers;
    var targetPaths = admin.brokerFolders(admin.brokerIds()).get(targetBrokers.iterator().next());

    if (!args.toBrokers.isEmpty() && !admin.brokerIds().containsAll(args.toBrokers))
      throw new IllegalArgumentException(
          "those brokers: "
              + args.toBrokers.stream()
                  .filter(i -> !admin.brokerIds().contains(i))
                  .map(String::valueOf)
                  .collect(Collectors.joining(","))
              + " are nonexistent");
    if (!args.partitions.isEmpty()
        && !admin.replicas(topics).keySet().stream()
            .map(TopicPartition::partition)
            .collect(Collectors.toSet())
            .containsAll(args.partitions))
      throw new IllegalArgumentException(
          "Topic "
              + topics.iterator().next()
              + " does not exist partition: "
              + args.partitions.toString());
    if (args.fromBrokers.containsAll(admin.brokerIds()))
      throw new IllegalArgumentException(
          "No enough available brokers!" + " removed at least one: " + args.fromBrokers);
    if (path != null) {
      if (!args.toBrokers.isEmpty() && !targetPaths.contains(path))
        throw new IllegalArgumentException(
            "path: " + path + " is not in broker" + args.toBrokers.iterator().next());
      if (args.toBrokers.isEmpty() && !targetPaths.contains(path))
        throw new IllegalArgumentException(
            "path: " + path + " is not in broker" + args.fromBrokers.iterator().next());
    }
  }

  static TreeMap<TopicPartition, Map.Entry<Integer, Integer>> migratorBrokerCheck(
      TopicAdmin admin, Parameters parameters) {
    TreeMap<TopicPartition, Map.Entry<Integer, Integer>> brokerMigrator =
        new TreeMap<>(
            Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
    admin.replicas(parameters.topics).entrySet().stream()
        .filter(
            tp ->
                ((parameters.partitions.contains(tp.getKey().partition()))
                        || parameters.partitions.isEmpty())
                    && parameters.fromBrokers.contains(tp.getValue().get(0).broker())
                    && !parameters.fromBrokers.containsAll(parameters.targetBrokers))
        .collect(Collectors.toSet())
        .forEach(
            (tp) -> {
              var currentBrokers =
                  tp.getValue().stream().map(Replica::broker).collect(Collectors.toSet());
              var keptBrokers =
                  currentBrokers.stream()
                      .filter(i -> !parameters.fromBrokers.contains(i))
                      .collect(Collectors.toSet());
              var numberOfMigratedReplicas = currentBrokers.size() - keptBrokers.size();
              if (numberOfMigratedReplicas > 0) {
                brokerMigrator.put(
                    tp.getKey(),
                    Map.entry(
                        currentBrokers.iterator().next(),
                        parameters.targetBrokers.iterator().next()));
              } else {
                brokerMigrator.put(
                    tp.getKey(),
                    Map.entry(currentBrokers.iterator().next(), currentBrokers.iterator().next()));
              }
            });
    return brokerMigrator;
  }

  static TreeMap<TopicPartition, Map.Entry<String, String>> migratorPathCheck(
      TopicAdmin admin, Parameters parameters) {
    TreeMap<TopicPartition, Map.Entry<String, String>> partitionMigrator =
        new TreeMap<>(
            Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
    admin.replicas(parameters.topics).entrySet().stream()
        .filter(
            t ->
                parameters.fromBrokers.contains(t.getValue().get(0).broker())
                    && parameters.topics.contains(t.getKey().topic())
                    && ((parameters.partitions.isEmpty())
                        || parameters.partitions.contains(t.getKey().partition())))
        .collect(Collectors.toList())
        .forEach(
            (tp) -> {
              var currentPath = Set.of(tp.getValue().get(0).path());
              if (parameters.path != null) {
                if (!currentPath.iterator().next().equals(parameters.path))
                  partitionMigrator.put(
                      tp.getKey(), Map.entry(currentPath.iterator().next(), parameters.path));
              } else {
                if (parameters.fromBrokers.contains(parameters.targetBrokers.iterator().next())) {
                  partitionMigrator.put(
                      tp.getKey(),
                      Map.entry(
                          currentPath.iterator().next(),
                          admin
                              .brokerFolders(parameters.targetBrokers)
                              .get(parameters.targetBrokers.iterator().next())
                              .stream()
                              .filter(p -> !currentPath.contains(p))
                              .collect(Collectors.toSet())
                              .iterator()
                              .next()));
                } else {
                  partitionMigrator.put(
                      tp.getKey(), Map.entry(currentPath.iterator().next(), "unknown"));
                }
              }
            });
    return partitionMigrator;
  }

  static Map<TopicPartition, MigratorInfo> execute(TopicAdmin admin, Argument args) {
    illegalArgs(admin, args);
    Parameters parameters = setParameters(admin, args);
    Map<TopicPartition, MigratorInfo> result =
        new TreeMap<>(
            Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
    var brokerMigrator = migratorBrokerCheck(admin, parameters);
    var partitionMigrator = migratorPathCheck(admin, parameters);

    brokerMigrator.forEach(
        (tp, assignments) -> {
          if (!assignments.getKey().equals(assignments.getValue())) {
            if (!args.verify)
              admin
                  .migrator()
                  .partition(tp.topic(), tp.partition())
                  .moveTo(Set.of(assignments.getValue()));
            if (!partitionMigrator.isEmpty()) {
              MigratorInfo migratorInfo = new MigratorInfo();
              migratorInfo.brokerIdA = assignments.getKey();
              migratorInfo.brokerIdB = assignments.getValue();
              migratorInfo.pathA = partitionMigrator.get(tp).getKey();
              migratorInfo.pathB = partitionMigrator.get(tp).getValue();
              result.put(tp, migratorInfo);
            }
          }
        });
    partitionMigrator.forEach(
        (tp, assignments) -> {
          if (!args.verify)
            admin
                .migrator()
                .partition(tp.topic(), tp.partition())
                .moveTo(Map.of(parameters.targetBrokers.iterator().next(), assignments.getValue()));
          int fromBroker, toBroker;
          if (parameters.fromBrokers.containsAll(parameters.targetBrokers)) {
            fromBroker = parameters.fromBrokers.iterator().next();
            toBroker = fromBroker;
          } else {
            fromBroker = brokerMigrator.get(tp).getKey();
            toBroker = brokerMigrator.get(tp).getValue();
          }
          if (!result.containsKey(tp)) {
            MigratorInfo migratorInfo = new MigratorInfo();
            migratorInfo.brokerIdA = fromBroker;
            migratorInfo.brokerIdB = toBroker;
            migratorInfo.pathA = assignments.getKey();
            migratorInfo.pathB = assignments.getValue();
            result.put(tp, migratorInfo);
          } else {
            if (assignments.getValue().equals("unknown")) {
              var newPath =
                  admin.replicas(parameters.topics).get(tp).size() == 2
                      ? admin.replicas(parameters.topics).get(tp).stream()
                          .filter(i -> !i.path().contains(assignments.getKey()))
                          .collect(Collectors.toSet())
                          .iterator()
                          .next()
                          .path()
                      : admin.replicas(parameters.topics).get(tp).get(0).path();
              if (assignments.getKey().equals(newPath)) {
                if (!args.verify) result.remove(tp);
              } else {
                MigratorInfo migratorInfo = new MigratorInfo();
                migratorInfo.brokerIdA = fromBroker;
                migratorInfo.brokerIdB = toBroker;
                migratorInfo.pathA = assignments.getKey();
                migratorInfo.pathB = newPath;
                result.put(tp, migratorInfo);
              }
            }
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
                          + assignments.brokerIdA
                          + ","
                          + assignments.pathA
                          + " after: "
                          + assignments.brokerIdB
                          + ","
                          + assignments.pathB));
    }
  }

  static class Argument extends BasicArgumentWithPropFile {
    @Parameter(
        names = {"--topics"},
        description = "Those topics' partitions will get reassigned. Empty means all topics",
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
        names = {"--partitions"},
        description = "all partitions that will be moved",
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
