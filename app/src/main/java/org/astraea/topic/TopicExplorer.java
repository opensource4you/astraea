package org.astraea.topic;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.io.PrintStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.common.TopicPartition;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicArgumentWithPropFile;
import org.astraea.utils.DataSize;
import org.astraea.utils.DataUnit;

public class TopicExplorer {

  static class PartitionInfo {
    final TopicPartition topicPartition;
    final long earliestOffset;
    final long latestOffset;
    final List<Group> consumerGroups;
    final List<Replica> replicas;

    public PartitionInfo(
        TopicPartition topicPartition,
        long earliestOffset,
        long latestOffset,
        List<Group> consumerGroups,
        List<Replica> replicas) {
      this.topicPartition = topicPartition;
      this.earliestOffset = earliestOffset;
      this.latestOffset = latestOffset;
      this.consumerGroups =
          consumerGroups.stream()
              .sorted(Comparator.comparing(Group::groupId))
              .collect(Collectors.toList());
      this.replicas =
          replicas.stream()
              .sorted(Comparator.comparing(Replica::broker))
              .collect(Collectors.toList());
    }
  }

  static class Result {
    public final LocalDateTime time;
    public final Map<String, List<PartitionInfo>> partitionInfo;
    public final Map<String, List<Member>> consumerGroupMembers;

    Result(
        LocalDateTime time,
        Map<String, List<PartitionInfo>> partitionInfo,
        Map<String, List<Member>> consumerGroupMembers) {
      this.time = time;
      this.partitionInfo = partitionInfo;
      this.consumerGroupMembers = consumerGroupMembers;
    }
  }

  static Result execute(TopicAdmin admin, Set<String> topics) {
    var replicas = admin.replicas(topics);
    var offsets = admin.offsets(topics);
    var consumerProgress = admin.partitionConsumerOffset(topics);
    var consumerGroupMembers =
        admin.consumerGroupMembers(
            consumerProgress.values().stream()
                .flatMap(Collection::stream)
                .map(Group::groupId)
                .collect(Collectors.toUnmodifiableSet()));
    var time = LocalDateTime.now();

    // Given topic name, return the partition count
    var topicPartitionCount =
        replicas.keySet().stream()
            .collect(Collectors.groupingBy(TopicPartition::topic))
            .entrySet()
            .stream()
            .map(entry -> Map.entry(entry.getKey(), entry.getValue().size()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    var topicPartitionInfos =
        topics.stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    (topic) ->
                        IntStream.range(0, topicPartitionCount.get(topic))
                            .mapToObj(partition -> new TopicPartition(topic, partition))
                            .map(
                                topicPartition ->
                                    new PartitionInfo(
                                        topicPartition,
                                        offsets.get(topicPartition).earliest(),
                                        offsets.get(topicPartition).latest(),
                                        consumerProgress.getOrDefault(topicPartition, List.of()),
                                        replicas.getOrDefault(topicPartition, List.of())))
                            .collect(Collectors.toUnmodifiableList())));

    return new Result(time, topicPartitionInfos, consumerGroupMembers);
  }

  public static void main(String[] args) throws IOException {
    var argument = ArgumentUtil.parseArgument(new Argument(), args);
    try (var admin = TopicAdmin.of(argument.props())) {
      var result = execute(admin, argument.topics.isEmpty() ? admin.topicNames() : argument.topics);
      TreeOutput.print(result, System.out);
    }
  }

  static class TreeOutput {

    private final LocalDateTime time;
    private final Map<String, List<PartitionInfo>> info;
    private final Map<String, List<Member>> consumerGroupMembers;
    private final PrintStream printStream;

    private static final String NEXT_LEVEL = "| ";
    private static final String TERMINATOR =
        "|_____________________________________________________________________________________";

    private TreeOutput(Result result, PrintStream printStream) {
      this.time = result.time;
      this.info = result.partitionInfo;
      this.consumerGroupMembers = result.consumerGroupMembers;
      this.printStream = printStream;
    }

    public static void print(Result result, PrintStream printStream) {
      new TreeOutput(result, printStream).print();
    }

    private final Stack<String> treePrefix = new Stack<>();
    private String cachedPrefix = "";

    private void nextLevel(String prefix, Runnable runnable) {
      treePrefix.push(prefix);
      cachedPrefix = String.join("", treePrefix);
      runnable.run();
      treePrefix.pop();
      cachedPrefix = String.join("", treePrefix);
    }

    private void nextLevel(String prefix, Runnable runnable, String terminator) {
      treePrefix.push(prefix);
      cachedPrefix = String.join("", treePrefix);
      runnable.run();
      treePrintln(terminator);
      treePrefix.pop();
      cachedPrefix = String.join("", treePrefix);
    }

    private void treePrintln(String format, Object... args) {
      printStream.print(cachedPrefix);
      printStream.printf(format, args);
      printStream.println();
    }

    public void print() {
      treePrefix.clear();
      cachedPrefix = "";

      treePrintln("[%s]", time.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
      nextLevel(
          " ",
          () -> {
            info.forEach(
                (topic, partitionInfos) -> {
                  treePrintln("Topic \"%s\"", topic);
                  nextLevel(
                      "",
                      () -> {
                        nextLevel(
                            NEXT_LEVEL,
                            () -> {
                              printConsumerGroup(topic, partitionInfos);
                              printPartitionReplica(partitionInfos);
                            });
                      },
                      TERMINATOR);
                });
          });
    }

    private void printConsumerGroup(String topic, List<PartitionInfo> partitionInfos) {
      treePrintln("Consumer Groups:");
      nextLevel(
          NEXT_LEVEL,
          () -> {

            // print out the progress of each group within the topic
            var consumerGroups =
                partitionInfos.stream()
                    .flatMap(x -> x.consumerGroups.stream())
                    .map(Group::groupId)
                    .sorted()
                    .distinct()
                    .collect(Collectors.toList());

            if (consumerGroups.isEmpty()) {
              treePrintln("no consumer group.");
            } else {
              consumerGroups.forEach(
                  groupId -> {

                    // print out consumer group
                    treePrintln("Consumer Group \"%s\"", groupId);
                    nextLevel(
                        "  ",
                        () -> {
                          var groups =
                              partitionInfos.stream()
                                  .collect(
                                      Collectors.toMap(
                                          (p) -> p.topicPartition.partition(),
                                          (p) ->
                                              p.consumerGroups.stream()
                                                  .filter(x -> x.groupId().equals(groupId))
                                                  .findFirst()
                                                  .orElseThrow()));

                          IntStream.range(0, partitionInfos.size())
                              .mapToObj(x -> new ConsumeProgress(topic, x, groups.get(x), info))
                              .forEach(x -> treePrintln("%s", x));

                          // print out the active member of this consumer group
                          treePrintln("Members:");
                          nextLevel(
                              "  ",
                              () -> {
                                var dutyOfMembers =
                                    groups.values().stream()
                                        .flatMap(x -> x.members().stream())
                                        .distinct()
                                        .collect(
                                            Collectors.toMap(
                                                (m) -> m,
                                                (m) ->
                                                    groups.entrySet().stream()
                                                        .filter(
                                                            g -> g.getValue().members().contains(m))
                                                        .map(Map.Entry::getKey)
                                                        .collect(Collectors.toList())));

                                // for each member that have no work, join them into dutyOfMembers
                                // with no partition assigned
                                consumerGroupMembers.get(groupId).stream()
                                    .filter(x -> !dutyOfMembers.containsKey(x))
                                    .forEach(x -> dutyOfMembers.put(x, List.of()));

                                if (dutyOfMembers.isEmpty()) treePrintln("no active member.");
                                else {
                                  dutyOfMembers.keySet().stream()
                                      .sorted(Comparator.comparing(Member::memberId))
                                      .forEach(
                                          member -> {
                                            treePrintln("member \"%s\"", member.memberId());
                                            nextLevel(
                                                NEXT_LEVEL,
                                                () -> {
                                                  if (dutyOfMembers.get(member).size() == 0)
                                                    treePrintln("no partition assigned.");
                                                  else
                                                    treePrintln(
                                                        "working on partition %s.",
                                                        dutyOfMembers.get(member).stream()
                                                            .sorted()
                                                            .map(Object::toString)
                                                            .collect(Collectors.joining(", ")));
                                                  treePrintln(
                                                      "clientId: \"%s\"", member.clientId());
                                                  treePrintln("host: \"%s\"", member.host());
                                                  if (member.groupInstanceId().isPresent())
                                                    treePrintln(
                                                        "groupInstanceId: \"%s\"",
                                                        member.groupInstanceId().get());
                                                  else treePrintln("groupInstanceId: none");
                                                });
                                          });
                                }
                              });
                        });
                  });
            }
          });
    }

    private void printPartitionReplica(List<PartitionInfo> partitionInfos) {
      treePrintln("Partitions/Replicas:");
      nextLevel(
          NEXT_LEVEL,
          () -> {
            // print partition & replica info
            partitionInfos.forEach(
                partitionInfo -> {
                  treePrintln(
                      "Partition \"%d\" (offset range: [%d, %d])",
                      partitionInfo.topicPartition.partition(),
                      partitionInfo.earliestOffset,
                      partitionInfo.latestOffset);
                  nextLevel(
                      "  ",
                      () -> {
                        treePrintln("Replicas:");
                        nextLevel(
                            NEXT_LEVEL,
                            () -> {
                              partitionInfo.replicas.stream()
                                  .sorted(Comparator.comparing(Replica::broker))
                                  .forEach(
                                      replica -> {
                                        treePrintln(
                                            "replica on broker %-4s %17s %s at \"%s\"",
                                            "#" + replica.broker(),
                                            ReplicaHelper.size(replica.size()),
                                            ReplicaHelper.descriptor(replica),
                                            replica.path());
                                      });
                            });
                      });
                });
          });
    }

    private static class ConsumeProgress {
      private final String topic;
      private final int index;
      private final Group group;
      private final Map<String, List<PartitionInfo>> map;

      private ConsumeProgress(
          String topic, int partition, Group group, Map<String, List<PartitionInfo>> map) {
        this.topic = topic;
        this.index = partition;
        this.group = group;
        this.map = map;
      }

      private long earliest() {
        return map.get(topic).get(index).earliestOffset;
      }

      private long latest() {
        return map.get(topic).get(index).latestOffset;
      }

      private long current() {
        return group.offset().orElse(0);
      }

      private String progressBar() {
        int totalBlocks = 20;
        int filledBlocks =
            Math.min(
                (int) (20.0 * ((double) (current() - earliest()) / (latest() - earliest()))), 20);
        int emptyBlocks = totalBlocks - filledBlocks;
        return Stream.concat(
                Collections.nCopies(filledBlocks, "#").stream(),
                Collections.nCopies(emptyBlocks, " ").stream())
            .collect(Collectors.joining("", "[", "]"));
      }

      @Override
      public String toString() {
        var partitionDigits = Math.max((int) (Math.log10(map.get(topic).size() - 1)) + 1, 1);
        return String.format(
            "consume progress of partition %"
                + partitionDigits
                + "d %s (earliest/current/latest offset %d/%d/%d)",
            index,
            progressBar(),
            earliest(),
            current(),
            latest());
      }
    }

    private static class ReplicaHelper {
      static String dataSizeString(DataSize dataSize) {
        return String.format(
            "%.2f %s", dataSize.idealMeasurement().doubleValue(), dataSize.idealDataUnit());
      }

      static String size(long bytes) {
        var dataSize = DataUnit.Byte.of(bytes);
        return "(size=" + dataSizeString(dataSize) + ")";
      }

      static String descriptor(Replica replica) {
        return Stream.of(
                Optional.ofNullable(replica.leader() ? "leader" : null),
                Optional.ofNullable(replica.lag() > 0 ? "lagged" + size(replica.lag()) : null),
                Optional.ofNullable(replica.inSync() ? null : "non-synced"),
                Optional.ofNullable(replica.isFuture() ? "future" : null))
            .flatMap(Optional::stream)
            .collect(Collectors.joining(", ", "[", "]"));
      }
    }
  }

  static class Argument extends BasicArgumentWithPropFile {
    @Parameter(
        names = {"--topics"},
        description = "the topics to show all offset-related information. Empty means all topics",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = ArgumentUtil.StringSetConverter.class)
    public Set<String> topics = Collections.emptySet();
  }
}
