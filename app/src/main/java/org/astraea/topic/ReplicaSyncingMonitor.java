package org.astraea.topic;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.Utils;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicArgumentWithPropFile;

public class ReplicaSyncingMonitor {

  public static void main(String[] args) {
    Argument argument = ArgumentUtil.parseArgument(new Argument(), args);
    try (TopicAdmin topicAdmin = TopicAdmin.of(argument.props())) {
      execute(topicAdmin, argument);
    } catch (IOException ioException) {
      ioException.printStackTrace();
    }
  }

  static void execute(final TopicAdmin topicAdmin, final Argument argument) {

    // this supplier will gives you all the topic name that the client interest in.
    Supplier<Set<String>> topicToTrack =
        () ->
            argument.topics.contains(Argument.EVERY_TOPIC)
                ? topicAdmin.topicNames()
                : argument.topics;

    // the non-synced topic-partition we want to monitor
    Set<TopicPartition> topicPartitionToTrack =
        findNonSyncedTopicPartition(topicAdmin, topicToTrack.get());

    // keep tracking the previous replica size of a topic-partition-replica tuple
    final Map<TopicPartitionReplica, Long> previousCheckedSize = new HashMap<>();

    while (!topicPartitionToTrack.isEmpty() || argument.keepTrack) {

      long startTime = System.nanoTime();

      System.out.printf(
          "[%s]%n", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
      var replicaProgress =
          topicAdmin.replicas(
              topicPartitionToTrack.stream()
                  .map(TopicPartition::topic)
                  .collect(Collectors.toUnmodifiableSet()));

      var topicPartitionLeaderReplicaTable =
          replicaProgress.entrySet().stream()
              .map(
                  x ->
                      Map.entry(
                          x.getKey(),
                          x.getValue().stream().filter(Replica::leader).findFirst().orElseThrow()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      topicPartitionToTrack.stream()
          .map(TopicPartition::topic)
          .distinct()
          .sorted()
          .forEachOrdered(
              topic -> {
                var partitionReplicas =
                    replicaProgress.entrySet().stream()
                        .filter(tpr -> tpr.getKey().topic().equals(topic))
                        .filter(tpr -> topicPartitionToTrack.contains(tpr.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                System.out.printf("  Topic \"%s\":%n", topic);

                partitionReplicas.keySet().stream()
                    .map(TopicPartition::partition)
                    .distinct()
                    .sorted()
                    .forEachOrdered(
                        partition -> {
                          TopicPartition tp = new TopicPartition(topic, partition);
                          Replica leaderReplica = topicPartitionLeaderReplicaTable.get(tp);
                          List<Replica> thisReplicas = partitionReplicas.get(tp);

                          // printing info
                          System.out.printf("  │ Partition %d:%n", partition);
                          thisReplicas.stream()
                              .map(
                                  replica ->
                                      Map.entry(
                                          new TopicPartitionReplica(
                                              topic, partition, replica.broker()),
                                          replica))
                              .map(
                                  entry ->
                                      Map.entry(
                                          entry.getValue(),
                                          new DataRate(
                                              leaderReplica.size(),
                                              previousCheckedSize.getOrDefault(
                                                  entry.getKey(), entry.getValue().size()),
                                              entry.getValue().size(),
                                              argument.interval)))
                              .map(
                                  entry ->
                                      String.format(
                                          "replica on broker %3d => %s",
                                          entry.getKey().broker(),
                                          formatString(entry.getKey(), entry.getValue())))
                              .map(s -> String.format("  │ │ %s", s))
                              .forEachOrdered(System.out::println);

                          // update previous size
                          thisReplicas.stream()
                              .map(
                                  replica ->
                                      Map.entry(
                                          new TopicPartitionReplica(
                                              topic, partition, replica.broker()),
                                          replica))
                              .forEach(
                                  entry ->
                                      previousCheckedSize.put(
                                          entry.getKey(), entry.getValue().size()));
                        });
              });

      // remove synced topic-partition-replica
      var topicPartitionFinished =
          replicaProgress.entrySet().stream()
              .filter(x -> x.getValue().stream().allMatch(Replica::inSync))
              .map(Map.Entry::getKey)
              .collect(Collectors.toSet());
      topicPartitionToTrack.removeAll(topicPartitionFinished);

      if (topicPartitionToTrack.isEmpty()) {
        System.out.println("  Every replica is synced.");
      }
      System.out.println();

      // attempts to discover any non-synced replica if flag --keep-track is used
      if (argument.keepTrack) {
        // find new non-synced topic-partition
        Set<TopicPartition> nonSyncedTopicPartition =
            findNonSyncedTopicPartition(topicAdmin, topicToTrack.get());
        // add all the non-synced topic-partition into tracking
        topicPartitionToTrack.addAll(nonSyncedTopicPartition);
        // remove previous progress from size map
        previousCheckedSize.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            .entrySet()
            .stream()
            .filter(
                tpr ->
                    !nonSyncedTopicPartition.contains(
                        new TopicPartition(tpr.getKey().topic(), tpr.getKey().partition())))
            .distinct()
            .forEach(tpr -> previousCheckedSize.remove(tpr.getKey()));
      }

      Utils.handleException(
          () -> {
            long expectedWaitNs = argument.interval * 1000L * 1000L;
            long elapsedNs = (System.nanoTime() - startTime);
            TimeUnit.NANOSECONDS.sleep(Math.max(expectedWaitNs - elapsedNs, 0));
            return 0;
          });
    }
  }

  private static String formatString(Replica replica, DataRate value) {
    return Stream.of(
            Optional.ofNullable(value.progressBar()),
            Optional.ofNullable(replica.leader() ? null : value.dataRateString()),
            Optional.ofNullable(
                replica.leader() || replica.inSync() ? null : "(" + value.estimateFinishTimeString() + ")"),
            Optional.ofNullable(replicaDescriptor(replica)))
        .flatMap(Optional::stream)
        .collect(Collectors.joining(" "));
  }

  static Set<TopicPartition> findNonSyncedTopicPartition(
      TopicAdmin topicAdmin, Set<String> topicToTrack) {
    return topicAdmin.replicas(topicToTrack).entrySet().stream()
        .filter(tpr -> tpr.getValue().stream().anyMatch(replica -> !replica.inSync()))
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }

  static class DataRate {
    public final long leaderSize;
    public final long previousSize;
    public final long currentSize;
    public final int interval;

    DataRate(long leaderSize, long previousSize, long currentSize, int intervalMs) {
      if (previousSize > currentSize) throw new IllegalArgumentException();
      this.leaderSize = leaderSize;
      this.previousSize = previousSize;
      this.currentSize = currentSize;
      this.interval = intervalMs;
    }

    public double progress() {
      return ((double) currentSize) / leaderSize * 100.0;
    }

    public String progressBar() {
      final int totalBlocks = 20;
      final int filledBlocks =
          (int) Math.min(totalBlocks, Math.floor(0.2 + progress() / (100.0 / totalBlocks)));
      final int emptyBlocks = totalBlocks - filledBlocks;

      return String.format(
          "[%s%s] %6.2f%%",
          String.join("", Collections.nCopies(filledBlocks, "#")),
          String.join("", Collections.nCopies(emptyBlocks, " ")),
          progress());
    }

    public double dataRatePerSec() {
      return (double) (currentSize - previousSize) / interval * 1000;
    }

    /**
     * estimated finish time, return a negative duration if the progress is stalled
     *
     * @return a {@link Duration} represent the estimated finish time, negative if progress is
     *     stalled.
     */
    public Duration estimateFinishTime() {
      if (dataRatePerSec() == 0) return Duration.ofSeconds(-1);
      return Duration.ofSeconds((long) ((leaderSize - currentSize) / dataRatePerSec()));
    }

    public String estimateFinishTimeString() {
      Duration estimatedTime = estimateFinishTime();
      return estimatedTime.isNegative()
          ? "unknown"
          : estimatedTime.toSeconds() == 0
              ? "about now"
              : Stream.of(
                      Map.entry(estimatedTime.toHoursPart(), "h"),
                      Map.entry(estimatedTime.toMinutesPart(), "m"),
                      Map.entry(estimatedTime.toSecondsPart(), "s"))
                  .dropWhile(x -> x.getKey() == 0)
                  .map(x -> x.getKey().toString() + x.getValue())
                  .collect(Collectors.joining(" ", "", " estimated"));
    }

    public String dataRateString() {
      final double sizeProgress = dataRatePerSec();
      final long TB = 1024L * 1024L * 1024L * 1024L;
      final long GB = 1024L * 1024L * 1024L;
      final long MB = 1024L * 1024L;
      final long KB = 1024L;
      if (sizeProgress > TB) return String.format("%.2f TB/s", sizeProgress / TB);
      else if (sizeProgress > GB) return String.format("%.2f GB/s", sizeProgress / GB);
      else if (sizeProgress > MB) return String.format("%.2f MB/s", sizeProgress / MB);
      else if (sizeProgress > KB) return String.format("%.2f KB/s", sizeProgress / KB);
      else return String.format("%.2f B/s", sizeProgress);
    }

    @Override
    public String toString() {
      if (estimateFinishTime().isZero() && currentSize == previousSize) return dataRateString();
      else return String.format("%s (%s)", dataRateString(), estimateFinishTimeString());
    }
  }

  static String replicaDescriptor(Replica replica) {
    return Stream.of(
            Optional.ofNullable(replica.leader() ? "leader" : null),
            Optional.ofNullable(replica.inSync() ? "synced" : null),
            Optional.ofNullable(replica.lag() > 0 ? "lagged" : null))
        .flatMap(Optional::stream)
        .collect(Collectors.joining(", ", "[", "]"));
  }

  static class Argument extends BasicArgumentWithPropFile {

    public static final String EVERY_TOPIC = "every non-synced topics...";

    @Parameter(
        names = {"--topic"},
        description = "String: topics to track",
        validateWith = ArgumentUtil.NotEmptyString.class)
    public Set<String> topics = Set.of(EVERY_TOPIC);

    @Parameter(
        names = {"--keep-track"},
        description =
            "Boolean: keep tracking even if all the replicas are synced, also attempts to discovery any non-synced replicas")
    public boolean keepTrack = false;

    @Parameter(
        names = {"--interval"},
        description =
            "Decimal: the frequency(time interval) to check replica state, support floating point value",
        converter = MillisecondConverter.class)
    public int interval = 1000;

    public static class MillisecondConverter implements IStringConverter<Integer> {
      @Override
      public Integer convert(String value) {
        return (int) (Double.parseDouble(value) * 1000);
      }
    }
  }
}
