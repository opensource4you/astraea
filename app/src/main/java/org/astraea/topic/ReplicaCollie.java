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
    Set<Integer> brokerSource;
    Set<Integer> brokerSink;
    Set<String> pathSource;
    Set<String> pathSink;
  }

  static Argument setArguments(TopicAdmin admin, Argument args) {
    Argument argument = new Argument();
    argument.topics = args.topics.isEmpty() ? admin.topicNames() : args.topics;
    argument.fromBrokers = args.fromBrokers;
    argument.partitions = args.partitions;
    argument.path = args.path.isEmpty() ? null : args.path;
    argument.verify=args.verify;
    if (args.partitions.isEmpty()
        && args.path.isEmpty()
        && args.toBrokers
            .isEmpty()) { // 若未同時指定partitions,搬移路徑與的的broker,則將該broker指定的topics搬移至其他broker(若未指定則包含該broker的所有topics)
      argument.toBrokers =
          admin.brokerIds().stream()
              .filter(b -> !args.fromBrokers.contains(b))
              .collect(Collectors.toSet());
    } else {
      argument.toBrokers = args.toBrokers.isEmpty() ? args.fromBrokers : args.toBrokers;
    }
    return argument;
  }

  static void checkArgs(TopicAdmin admin, Argument args) {
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
    if(args.topics.size()>=2 && !args.partitions.isEmpty())
      throw new IllegalArgumentException(
              "When specifying multiple topics, --partitions cannot be specified.");
    if (path != null) {
      if (!args.toBrokers.isEmpty() && !targetPaths.contains(path))
        throw new IllegalArgumentException(
            "path: " + path + " is not in broker" + args.toBrokers.iterator().next());
      if (args.toBrokers.isEmpty() && !targetPaths.contains(path))
        throw new IllegalArgumentException(
            "path: " + path + " is not in broker" + args.fromBrokers.iterator().next());
    }
  }

  static TreeMap<TopicPartition, Map.Entry<Set<Integer>, Set<Integer>>> checkMigratorBroker(
      TopicAdmin admin, Argument argument) {
    TreeMap<TopicPartition, Map.Entry<Set<Integer>, Set<Integer>>> brokerMigrate =
        new TreeMap<>(
            Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
    admin.replicas(argument.topics).entrySet().stream()
        .filter(
            tp ->
                ((argument.partitions.contains(tp.getKey().partition()))
                        || argument.partitions.isEmpty())
                    && !argument.fromBrokers.containsAll(argument.toBrokers))
        .collect(Collectors.toSet())
        .forEach(
            (tp) -> {
              var currentBrokers =
                  tp.getValue().stream().map(Replica::broker).collect(Collectors.toSet());
              var keptBrokers =
                  currentBrokers.stream()
                      .filter(i -> !argument.fromBrokers.contains(i))
                      .collect(Collectors.toSet());
              var numberOfMigratedReplicas = currentBrokers.size() - keptBrokers.size();
              if (numberOfMigratedReplicas > 0) {
                var availableBrokers =
                        argument.toBrokers.stream()
                                .filter(i -> !keptBrokers.contains(i) && !argument.fromBrokers.contains(i))
                                .collect(Collectors.toSet());
                if (availableBrokers.size() < numberOfMigratedReplicas)
                  throw new IllegalArgumentException(
                          "No enough available brokers! Available: "
                                  + argument.toBrokers
                                  + " current: "
                                  + currentBrokers
                                  + " removed: "
                                  + argument.fromBrokers);
                brokerMigrate.put(
                    tp.getKey(),
                    Map.entry(
                        currentBrokers, argument.toBrokers));
              } else {
                brokerMigrate.put(
                    tp.getKey(),
                    Map.entry(currentBrokers, currentBrokers));
              }
            });
    return brokerMigrate;
  }

  static TreeMap<TopicPartition, Map.Entry<Set<String>, Set<String>>> checkMigratorPath(
      TopicAdmin admin, Argument argument) {
    TreeMap<TopicPartition, Map.Entry<Set<String>,Set<String>>> pathMigrate =
        new TreeMap<>(
            Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
   admin.replicas(argument.topics).entrySet().stream()
            .filter(
            tp ->
                 argument.topics.contains(tp.getKey().topic())
                    && ((argument.partitions.isEmpty())
                        || argument.partitions.contains(tp.getKey().partition())))
        .collect(Collectors.toList())
        .forEach(
            (tp) -> {
              Set<String> fromPath=new HashSet<>();
              Set<String> toPath=new HashSet<>();
              tp.getValue().stream().filter(assignments-> argument.fromBrokers.contains(assignments.broker()))
                      .forEach(assignments -> {
                var currentPath = assignments.path();
                fromPath.add(currentPath);
                if (argument.path != null) {
                  if (!currentPath.equals(argument.path.iterator().next()))
                    toPath.add(argument.path.iterator().next());
                } else {
                  if (argument.fromBrokers.contains(argument.toBrokers.iterator().next())) {
                    toPath.add(
                                    admin
                                            .brokerFolders(argument.toBrokers)
                                            .get(argument.toBrokers.iterator().next())
                                            .stream()
                                            .filter(p -> !currentPath.contains(p))
                                            .collect(Collectors.toSet()).iterator().next()
                            );
                  } else {
                    toPath.add("unknown");
                  }
                }
              });
              if(!fromPath.isEmpty())
              pathMigrate.put(tp.getKey(),Map.entry(fromPath,toPath));
            });
    return pathMigrate;
  }

  static void brokerMigrator(
          TreeMap<TopicPartition, Map.Entry<Set<Integer>, Set<Integer>>> brokerMigrate, TopicAdmin admin) {
    brokerMigrate.forEach(
        (tp, assignments) -> {
          if (!assignments.getKey().equals(assignments.getValue())) {
            admin
                .migrator()
                .partition(tp.topic(), tp.partition())
                .moveTo(assignments.getValue());
          }
        });
  }

  static void pathMigrator(
      TreeMap<TopicPartition, Map.Entry<Set<String>, Set<String>>> pathMigrate,
      TopicAdmin admin,
      Integer broker) {
    pathMigrate.forEach(
        (tp, assignments) -> {
          if(assignments.getValue().size()>0)
          admin
              .migrator()
              .partition(tp.topic(), tp.partition())
              .moveTo(Map.of(broker, assignments.getValue().iterator().next()));
        });
  }

  static TreeMap<TopicPartition, MigratorInfo> getResult(
          TreeMap<TopicPartition, Map.Entry<Set<Integer>, Set<Integer>>> brokerMigrate,
          TreeMap<TopicPartition, Map.Entry<Set<String>, Set<String>>> pathMigrate,Argument argument,TopicAdmin admin) {
    var result =
        new TreeMap<TopicPartition, MigratorInfo>(
            Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
    brokerMigrate.forEach(
        (tp, assignments) -> {
          if (!assignments.getKey().equals(assignments.getValue())) {
            if (!pathMigrate.isEmpty()) {
              MigratorInfo migratorInfo = new MigratorInfo();
              migratorInfo.brokerSource = assignments.getKey();
              migratorInfo.brokerSink = assignments.getValue();
              migratorInfo.pathSource = pathMigrate.get(tp).getKey();
              migratorInfo.pathSink = pathMigrate.get(tp).getValue();
              result.put(tp, migratorInfo);
            }
          }
        });
    pathMigrate.forEach(
            (tp, assignments) -> {
              Set<Integer> fromBroker;
              Set<Integer> toBroker;
              if (argument.fromBrokers.containsAll(argument.toBrokers)) {
                fromBroker = argument.fromBrokers;
                toBroker = fromBroker;
              } else {
                fromBroker = brokerMigrate.get(tp).getKey();
                toBroker = brokerMigrate.get(tp).getValue();
              }
              if (!result.containsKey(tp)) {
                MigratorInfo migratorInfo = new MigratorInfo();
                migratorInfo.brokerSource = fromBroker;
                migratorInfo.brokerSink = toBroker;
                migratorInfo.pathSource = assignments.getKey();
                migratorInfo.pathSink = assignments.getValue();
                result.put(tp, migratorInfo);
              } else {
                if (assignments.getValue().contains("unknown")) {
                    Set<String > newPath=new HashSet<>();
                   newPath =!argument.verify ?
                          admin.replicas(argument.topics).get(tp).stream()
                                  .map(Replica::path).filter(path -> !assignments.getKey().contains(path))
                                  .collect(Collectors.toSet()) :Set.of("unknown");
                  if (assignments.getKey().equals(newPath)) {
                    if (!argument.verify) result.remove(tp);
                  } else {
                    MigratorInfo migratorInfo = new MigratorInfo();
                    migratorInfo.brokerSource = fromBroker;
                    migratorInfo.brokerSink = toBroker;
                    migratorInfo.pathSource = assignments.getKey();
                    migratorInfo.pathSink = newPath;
                    result.put(tp, migratorInfo);
                  }
                }
              }
            });
    return result;
  }

  static Map<TopicPartition, MigratorInfo> execute(TopicAdmin admin, Argument args) {
    checkArgs(admin, args);
    Argument argument = setArguments(admin, args);
    var brokerMigrate = checkMigratorBroker(admin, argument);
    var pathMigrate = checkMigratorPath(admin, argument);
    if (!argument.verify) {
      brokerMigrator(brokerMigrate, admin);
      pathMigrator(pathMigrate, admin, argument.toBrokers.iterator().next());
    }
    return getResult(brokerMigrate, pathMigrate,argument,admin);
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
                          + assignments.brokerSource
                          + ","
                          + assignments.pathSource
                          + " after: "
                          + assignments.brokerSink
                          + ","
                          + assignments.pathSink));
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
