package org.astraea.topic;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.io.PrintStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.common.TopicPartition;
import org.astraea.argument.NonEmptyStringField;
import org.astraea.argument.StringSetField;
import org.astraea.utils.DataSize;
import org.astraea.utils.DataUnit;

public class TopicExplorer {

  static class PartitionInfo {
    final TopicPartition topicPartition;
    final long earliestOffset;
    final long latestOffset;
    final List<Replica> replicas;

    public PartitionInfo(
        TopicPartition topicPartition,
        long earliestOffset,
        long latestOffset,
        List<Replica> replicas) {
      this.topicPartition = topicPartition;
      this.earliestOffset = earliestOffset;
      this.latestOffset = latestOffset;
      this.replicas =
          replicas.stream()
              .sorted(Comparator.comparing(Replica::broker))
              .collect(Collectors.toList());
    }
  }

  static class Result {
    public final LocalDateTime time;
    public final Map<String, List<PartitionInfo>> partitionInfo;
    public final Map<String, ConsumerGroup> consumerGroups;

    Result(
        LocalDateTime time,
        Map<String, List<PartitionInfo>> partitionInfo,
        Map<String, ConsumerGroup> consumerGroups) {
      this.time = time;
      this.partitionInfo = partitionInfo;
      this.consumerGroups = consumerGroups;
    }
  }

  static Result execute(TopicAdmin admin, Set<String> topics) {
    var replicas = admin.replicas(topics);

    var invalidTopics =
        replicas.entrySet().stream()
            .filter(x -> x.getValue().stream().noneMatch(Replica::leader))
            .map(Map.Entry::getKey)
            .sorted(
                Comparator.comparing(TopicPartition::topic)
                    .thenComparing(TopicPartition::partition))
            .collect(Collectors.toUnmodifiableList());
    if (!invalidTopics.isEmpty()) {
      // The metadata request sent by Kafka admin succeeds only if the leaders of all partitions are
      // alive. Hence, we throw exception here to make consistent behavior with Kafka.
      throw new IllegalStateException("Some partitions have no leader: " + invalidTopics);
    }

    var offsets = admin.offsets(topics);
    var consumerGroups = admin.consumerGroup(Set.of());
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
                                        replicas.getOrDefault(topicPartition, List.of())))
                            .collect(Collectors.toUnmodifiableList())));

    return new Result(time, topicPartitionInfos, consumerGroups);
  }

  public static void main(String[] args) throws IOException {
    var argument = org.astraea.argument.Argument.parse(new Argument(), args);
    try (var admin = TopicAdmin.of(argument.props())) {
      var result = execute(admin, argument.topics.isEmpty() ? admin.topicNames() : argument.topics);
      TreeOutput.print(result, System.out);
    }
  }

  static class TreeOutput {

    private final LocalDateTime time;
    private final Map<String, List<PartitionInfo>> info;
    private final Map<String, ConsumerGroup> consumerGroups;
    private final PrintStream printStream;

    private static final String NEXT_LEVEL = "| ";
    private static final String TERMINATOR =
        "|_____________________________________________________________________________________";

    private TreeOutput(Result result, PrintStream printStream) {
      this.time = Objects.requireNonNull(result.time);
      this.info = Map.copyOf(result.partitionInfo);
      this.consumerGroups = Map.copyOf(result.consumerGroups);
      this.printStream = Objects.requireNonNull(printStream);
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
          () ->
              info.entrySet().stream()
                  .sorted(Map.Entry.comparingByKey())
                  .forEach(
                      (entry) -> {
                        treePrintln("Topic \"%s\"", entry.getKey());
                        nextLevel(
                            "",
                            () ->
                                nextLevel(
                                    NEXT_LEVEL,
                                    () -> {
                                      printStatistics(entry.getKey(), entry.getValue());
                                      printConsumerGroup(entry.getKey(), entry.getValue());
                                      printPartitionReplica(entry.getValue());
                                    }),
                            TERMINATOR);
                      }));
    }

    private void printStatistics(String topic, List<PartitionInfo> partitionInfos) {
      treePrintln("Statistics of Topic \"%s\"", topic);
      nextLevel(
          NEXT_LEVEL,
          () -> {
            treePrintln(
                "Topic Size: %s",
                DataUnit.Byte.of(
                    partitionInfos.stream()
                        .flatMapToLong(x -> x.replicas.stream().mapToLong(Replica::size))
                        .sum()));
            treePrintln("Partition Count: %d", partitionInfos.size());
            treePrintln(
                "Partition Size Average: %s",
                DataUnit.Byte.of(
                    (long)
                        partitionInfos.stream()
                            .mapToLong(x -> x.replicas.stream().mapToLong(Replica::size).sum())
                            .summaryStatistics()
                            .getAverage()));
            treePrintln(
                "Replica Count: %d",
                partitionInfos.stream().mapToInt(x -> x.replicas.size()).sum());
            treePrintln(
                "Replica Size Average: %s",
                DataUnit.Byte.of(
                    (long)
                        partitionInfos.stream()
                            .flatMapToLong(x -> x.replicas.stream().mapToLong(Replica::size))
                            .summaryStatistics()
                            .getAverage()));
          });
    }

    private void printConsumerGroup(String topic, List<PartitionInfo> partitionInfos) {
      treePrintln("Consumer Groups:");
      nextLevel(
          NEXT_LEVEL,
          () -> {
            // find all the consumer groups that are related to this topic
            var consumerGroupList =
                consumerGroups.values().stream()
                    .filter(
                        consumerGroup ->
                            consumerGroup.consumeProgress().keySet().stream()
                                .anyMatch(tp -> tp.topic().equals(topic)))
                    .map(ConsumerGroup::groupId)
                    .distinct()
                    .map(consumerGroups::get)
                    .collect(Collectors.toUnmodifiableList());

            if (consumerGroupList.isEmpty()) {
              treePrintln("no consumer group.");
            } else {
              consumerGroupList.forEach(
                  consumerGroup -> {

                    // print out consumer group
                    treePrintln("Consumer Group \"%s\"", consumerGroup.groupId());
                    nextLevel(
                        "  ",
                        () -> {
                          IntStream.range(0, partitionInfos.size())
                              .mapToObj(x -> new ConsumeProgress(topic, x, consumerGroup, info))
                              .forEach(x -> treePrintln("%s", x));

                          // print out the active member of this consumer group
                          treePrintln("Members:");
                          nextLevel(
                              "  ",
                              () -> {
                                var memberAssignment =
                                    consumerGroup.activeMembers().stream()
                                        .collect(
                                            Collectors.toUnmodifiableMap(
                                                x -> x,
                                                x ->
                                                    consumerGroup
                                                        .assignment()
                                                        .getOrDefault(x, Set.of())
                                                        .stream()
                                                        .filter(tp -> tp.topic().equals(topic))
                                                        .map(TopicPartition::partition)
                                                        .collect(Collectors.toUnmodifiableSet())));

                                int totalAssignedPartition =
                                    memberAssignment.values().stream().mapToInt(Set::size).sum();

                                // how to tell if this consumer group is working on the specific
                                // 1. there must be some active member(of course).
                                // 2. at least one partition is assigned to some member. It is
                                // possible that a consumer group used to work on a specific topic
                                // in the past. But no longer work at it at this moment. In such a
                                // situation, Kafka API will still tell you the consumer group
                                // belongs to that topic(of course there are offsets stored here so
                                // Kafka cannot just delete it). But you will realize even though
                                // there are some live members here but none of them are assigned to
                                // the topic. Because they didn't declare to work on that topic(used
                                // to, but not now).
                                if (memberAssignment.isEmpty() || totalAssignedPartition == 0)
                                  treePrintln("no active member.");
                                else {
                                  memberAssignment.keySet().stream()
                                      .sorted(Comparator.comparing(Member::memberId))
                                      .forEach(
                                          member -> {
                                            treePrintln("member \"%s\"", member.memberId());
                                            nextLevel(
                                                NEXT_LEVEL,
                                                () -> {
                                                  if (memberAssignment.get(member).size() == 0)
                                                    treePrintln("no partition assigned.");
                                                  else
                                                    treePrintln(
                                                        "working on partition %s.",
                                                        memberAssignment.get(member).stream()
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
                      "Partition \"%d\" (size: %s) (offset range: [%d, %d])",
                      partitionInfo.topicPartition.partition(),
                      DataUnit.Byte.of(
                          partitionInfo.replicas.stream().mapToLong(Replica::size).sum()),
                      partitionInfo.earliestOffset,
                      partitionInfo.latestOffset);
                  nextLevel(
                      "  ",
                      () ->
                          nextLevel(
                              NEXT_LEVEL,
                              () ->
                                  partitionInfo.replicas.stream()
                                      .sorted(Comparator.comparing(Replica::broker))
                                      .forEach(
                                          replica ->
                                              treePrintln(
                                                  "replica on broker %-4s %17s %s at \"%s\"",
                                                  "#" + replica.broker(),
                                                  ReplicaHelper.size(replica.size()),
                                                  ReplicaHelper.descriptor(replica),
                                                  replica.path()))));
                });
          });
    }

    private static class ConsumeProgress {
      private final String topic;
      private final int index;
      private final ConsumerGroup group;
      private final Map<String, List<PartitionInfo>> map;

      private ConsumeProgress(
          String topic, int partition, ConsumerGroup group, Map<String, List<PartitionInfo>> map) {
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
        return group.consumeProgress().get(new TopicPartition(topic, index));
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
            "consume progress of partition #%"
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
                Optional.of(replica.leader() ? "leader" : "follower"),
                Optional.ofNullable(replica.lag() > 0 ? "lagged" + size(replica.lag()) : null),
                Optional.ofNullable(replica.inSync() ? null : "non-synced"),
                Optional.of(replica.isOffline() ? "offline" : "online"),
                Optional.ofNullable(replica.isFuture() ? "future" : null))
            .flatMap(Optional::stream)
            .collect(Collectors.joining(", ", "[", "]"));
      }
    }
  }

  static class Argument extends org.astraea.argument.Argument {
    @Parameter(
        names = {"--topics"},
        description = "the topics to show all offset-related information. Empty means all topics",
        validateWith = NonEmptyStringField.class,
        converter = StringSetField.class)
    public Set<String> topics = Collections.emptySet();
  }
}
