package org.astraea.topic;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicArgumentWithPropFile;

public class ReplicaCollie {
  static Map<TopicPartition, Map.Entry<Set<Integer>, Set<Integer>>> execute(
      TopicAdmin admin, Argument args) {
    var topics = args.topics.isEmpty() ? admin.topicNames() : args.topics;
    var allBrokers = admin.brokerIds();
    var path = args.path.isEmpty() ? null : args.path.iterator().next();
    var fromBroker = args.fromBrokers.iterator().next();
    var allTopicsPartitions =
        admin.replicas(topics).keySet().stream()
            .map(TopicPartition::partition)
            .collect(Collectors.toSet());
    var crossBroker = true;
    if (!args.toBrokers.isEmpty() && !allBrokers.containsAll(args.toBrokers))
      throw new IllegalArgumentException(
          "those brokers: "
              + args.toBrokers.stream()
                  .filter(i -> !allBrokers.contains(i))
                  .map(String::valueOf)
                  .collect(Collectors.joining(","))
              + " are nonexistent");
    if (!args.partition.isEmpty() && !allTopicsPartitions.containsAll(args.partition))
      throw new IllegalArgumentException(
          "Topic "
              + topics.iterator().next()
              + " does not exist partition: "
              + args.partition.toString());
    var targetBrokers = args.toBrokers.isEmpty() ? allBrokers : args.toBrokers;
    var result =
        new TreeMap<TopicPartition, Map.Entry<Set<Integer>, Set<Integer>>>(
            Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
    var result2 =
        new TreeMap<TopicPartition, Map.Entry<Set<String>, Set<String>>>(
            Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
    admin.replicas(topics).entrySet().stream()
        .filter(tp -> args.partition.contains(tp.getKey().partition()) || args.partition.isEmpty())
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
                result.put(tp.getKey(), Map.entry(currentBrokers, finalBrokers));
              }
            });
    if (result.isEmpty()) crossBroker = false;
    result.forEach(
        (tp, assignments) -> {
          if (!args.verify)
            admin.migrator().partition(tp.topic(), tp.partition()).moveTo(assignments.getValue());
        });
    if (path != null && !args.partition.isEmpty()) {
      if (!crossBroker && !admin.brokerFolders(allBrokers).get(fromBroker).contains(path)
          || crossBroker
              && !admin
                  .brokerFolders(allBrokers)
                  .get(targetBrokers.iterator().next())
                  .contains(path))
        throw new IllegalArgumentException("path: " + path + " is not in broker" + fromBroker);
      admin.replicas(topics).entrySet().stream()
          .filter(
              t ->
                  t.getKey().topic().equals(topics.iterator().next())
                      && args.partition.contains(t.getKey().partition()))
          .collect(Collectors.toList())
          .forEach(
              (tp) -> {
                var currentPath =
                    tp.getValue().stream().map(Replica::path).collect(Collectors.toSet());
                if (tp.getValue().get(0).broker() == 0)
                  if (topics.iterator().next().equals(tp.getKey().topic()))
                    if (!currentPath.equals(args.path))
                      result2.put(tp.getKey(), Map.entry(currentPath, args.path));
              });
    }
    return result;
  }

  public static void main(String[] args) throws IOException {
    var argument = ArgumentUtil.parseArgument(new Argument(), args);
    try (var admin = TopicAdmin.of(argument.props())) {
      var a = ReplicaCollie.<String>execute(admin, argument);
      var b = ReplicaCollie.<Integer>execute(admin, argument);
      a.forEach(
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
        names = {"--partition"},
        description = "A partition that will be moved",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = ArgumentUtil.IntegerSetConverter.class)
    Set<Integer> partition = Collections.emptySet();

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
