/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.admin;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.astraea.app.argument.BooleanField;
import org.astraea.app.argument.IntegerSetField;
import org.astraea.app.argument.StringSetField;

public class ReplicaCollie {
  static final String UNKNOWN = "unknown";

  static class MigratorInfo {
    List<Integer> brokerSource;
    List<Integer> brokerSink;
    Set<String> pathSource;
    Set<String> pathSink;
  }

  static Argument setArguments(Admin admin, Argument args) {
    Argument argument = new Argument();
    argument.topics = args.topics.isEmpty() ? admin.topicNames() : args.topics;
    argument.fromBrokers = args.fromBrokers;
    argument.partitions = args.partitions;
    argument.path = args.path.isEmpty() ? null : args.path;
    argument.verify = args.verify;
    // If the partitions and the broker of to migrate path are not specified
    // at the same time, the topics specified by the broker will be moved to
    // other brokers (if not specified, all topics of the broker will be
    // included).
    if (args.partitions.isEmpty() && args.path.isEmpty() && args.toBrokers.isEmpty()) {
      argument.toBrokers =
          admin.brokerIds().stream()
              .filter(b -> !args.fromBrokers.contains(b))
              .collect(Collectors.toList());
    } else {
      argument.toBrokers = args.toBrokers.isEmpty() ? args.fromBrokers : args.toBrokers;
    }
    return argument;
  }

  static void checkArgs(Admin admin, Argument args) {
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
    if (args.topics.size() >= 2 && !args.partitions.isEmpty())
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

  static TreeMap<TopicPartition, Map.Entry<List<Integer>, List<Integer>>> checkMigratorBroker(
      Admin admin, Argument argument) {
    var brokerMigrate = new TreeMap<TopicPartition, Map.Entry<List<Integer>, List<Integer>>>();
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
                  tp.getValue().stream().map(Replica::broker).collect(Collectors.toList());
              var keptBrokers =
                  currentBrokers.stream()
                      .filter(i -> !argument.fromBrokers.contains(i))
                      .collect(Collectors.toSet());
              var numberOfMigratedReplicas = currentBrokers.size() - keptBrokers.size();
              if (numberOfMigratedReplicas > 0) {
                var availableBrokers =
                    argument.toBrokers.stream()
                        .filter(i -> !keptBrokers.contains(i) && !argument.fromBrokers.contains(i))
                        .collect(Collectors.toList());
                if (availableBrokers.size() < numberOfMigratedReplicas)
                  throw new IllegalArgumentException(
                      "No enough available brokers! Available: "
                          + argument.toBrokers
                          + " current: "
                          + currentBrokers
                          + " removed: "
                          + argument.fromBrokers);
                var targetBrokers = new ArrayList<>(keptBrokers);
                if (numberOfMigratedReplicas < argument.toBrokers.size())
                  targetBrokers.addAll(
                      new ArrayList<>(availableBrokers)
                          .subList(0, argument.fromBrokers.size() - keptBrokers.size()));
                else
                  targetBrokers.addAll(
                      new ArrayList<>(availableBrokers).subList(0, argument.toBrokers.size()));
                brokerMigrate.put(tp.getKey(), Map.entry(argument.fromBrokers, targetBrokers));
              } else {
                brokerMigrate.put(tp.getKey(), Map.entry(currentBrokers, currentBrokers));
              }
            });
    return brokerMigrate;
  }

  static TreeMap<TopicPartition, Map.Entry<Set<String>, Set<String>>> checkMigratorPath(
      Admin admin, Argument argument) {
    var pathMigrate = new TreeMap<TopicPartition, Map.Entry<Set<String>, Set<String>>>();
    admin.replicas(argument.topics).entrySet().stream()
        .filter(
            tp ->
                argument.topics.contains(tp.getKey().topic())
                    && ((argument.partitions.isEmpty())
                        || argument.partitions.contains(tp.getKey().partition())))
        .collect(Collectors.toList())
        .forEach(
            (tp) -> {
              Set<String> fromPath = new HashSet<>();
              Set<String> toPath = new HashSet<>();
              tp.getValue().stream()
                  .filter(assignments -> argument.fromBrokers.contains(assignments.broker()))
                  .forEach(
                      assignments -> {
                        var currentPath = assignments.path();
                        fromPath.add(currentPath);
                        if (argument.path != null) {
                          if (!currentPath.equals(argument.path.iterator().next()))
                            toPath.add(argument.path.iterator().next());
                        } else {
                          if (argument.fromBrokers.contains(argument.toBrokers.iterator().next())) {
                            toPath.add(
                                admin
                                    .brokerFolders(new HashSet<>(argument.toBrokers))
                                    .get(argument.toBrokers.iterator().next())
                                    .stream()
                                    .filter(p -> !currentPath.contains(p))
                                    .collect(Collectors.toSet())
                                    .iterator()
                                    .next());
                          } else {
                            toPath.add(UNKNOWN);
                          }
                        }
                      });
              if (!fromPath.isEmpty()) pathMigrate.put(tp.getKey(), Map.entry(fromPath, toPath));
            });
    return pathMigrate;
  }

  static void brokerMigrator(
      TreeMap<TopicPartition, Map.Entry<List<Integer>, List<Integer>>> brokerMigrate, Admin admin) {
    brokerMigrate.forEach(
        (tp, assignments) -> {
          if (!assignments.getKey().equals(assignments.getValue())) {
            admin.migrator().partition(tp.topic(), tp.partition()).moveTo(assignments.getValue());
          }
        });
  }

  static void pathMigrator(
      TreeMap<TopicPartition, Map.Entry<Set<String>, Set<String>>> pathMigrate,
      Admin admin,
      Integer broker) {
    pathMigrate.forEach(
        (tp, assignments) -> {
          // TODO: this code is a bit ugly, but it is fine as we are going to remove this class.
          if (assignments.getValue().size() > 0
              && !assignments.getValue().iterator().next().equals(UNKNOWN))
            admin
                .migrator()
                .partition(tp.topic(), tp.partition())
                .moveTo(Map.of(broker, assignments.getValue().iterator().next()));
        });
  }

  static TreeMap<TopicPartition, MigratorInfo> getResult(
      TreeMap<TopicPartition, Map.Entry<List<Integer>, List<Integer>>> brokerMigrate,
      TreeMap<TopicPartition, Map.Entry<Set<String>, Set<String>>> pathMigrate,
      Argument argument,
      Admin admin) {
    var result = new TreeMap<TopicPartition, MigratorInfo>();
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
          List<Integer> fromBroker;
          List<Integer> toBroker;
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
            if (assignments.getValue().contains(UNKNOWN)) {
              Set<String> newPath;
              if (!argument.verify) {
                var replicas = admin.replicas(argument.topics).get(tp);
                newPath =
                    replicas.stream()
                            .map(Replica::path)
                            .filter(p -> !assignments.getKey().contains(p))
                            .collect(Collectors.toSet())
                            .isEmpty()
                        ? replicas.stream().map(Replica::path).collect(Collectors.toSet())
                        : replicas.stream()
                            .map(Replica::path)
                            .filter(p -> !assignments.getKey().contains(p))
                            .collect(Collectors.toSet());
              } else {
                newPath = Set.of(UNKNOWN);
              }
              if (assignments.getKey().equals(newPath) && fromBroker == toBroker) {
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

  static Map<TopicPartition, MigratorInfo> execute(Admin admin, Argument args) {
    checkArgs(admin, args);
    Argument argument = setArguments(admin, args);
    var brokerMigrate = checkMigratorBroker(admin, argument);
    var pathMigrate = checkMigratorPath(admin, argument);
    if (!argument.verify) {
      brokerMigrator(brokerMigrate, admin);
      pathMigrator(pathMigrate, admin, argument.toBrokers.iterator().next());
    }
    return getResult(brokerMigrate, pathMigrate, argument, admin);
  }

  public static void main(String[] args) throws IOException {
    var argument = org.astraea.app.argument.Argument.parse(new Argument(), args);

    try (var admin = Admin.of(argument.bootstrapServers())) {
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

  static class Argument extends org.astraea.app.argument.Argument {
    @Parameter(
        names = {"--topics"},
        description = "Those topics' partitions will get reassigned. Empty means all topics",
        validateWith = StringSetField.class,
        converter = StringSetField.class)
    public Set<String> topics = Collections.emptySet();

    @Parameter(
        names = {"--from"},
        description = "Those brokers won't hold any replicas of topics (defined by --topics)",
        required = true)
    List<Integer> fromBrokers;

    @Parameter(
        names = {"--to"},
        description = "The replicas of topics (defined by --topic) will be moved to those brokers")
    List<Integer> toBrokers = List.of();

    @Parameter(
        names = {"--partitions"},
        description = "all partitions that will be moved",
        validateWith = IntegerSetField.class,
        converter = IntegerSetField.class)
    Set<Integer> partitions = Collections.emptySet();

    @Parameter(
        names = {"--path"},
        description = "The partition that will be moved to",
        validateWith = StringSetField.class,
        converter = StringSetField.class)
    Set<String> path = Collections.emptySet();

    @Parameter(
        names = {"--verify"},
        description = "add this flag if all you want to do is to review the plan",
        validateWith = BooleanField.class,
        converter = BooleanField.class)
    boolean verify = false;
  }
}
