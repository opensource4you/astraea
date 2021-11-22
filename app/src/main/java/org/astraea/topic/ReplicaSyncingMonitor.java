package org.astraea.topic;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicArgumentWithPropFile;

public class ReplicaSyncingMonitor {

  public static void main(String[] args) {
    execute(ArgumentUtil.parseArgument(new Argument(), args));
  }

  static void execute(final Argument argument) {
    try (TopicAdmin topicAdmin = TopicAdmin.of(argument.props())) {

      Set<String> topicToTrack =
          argument.topics.contains(Argument.EVERY_TOPIC)
              ? topicAdmin.topicNames()
              : argument.topics;

      Set<TopicPartition> topicPartitionToTrack =
          topicAdmin.replicas(topicToTrack).entrySet().stream()
              .filter(tpr -> tpr.getValue().stream().anyMatch(replica -> !replica.inSync()))
              .map(Map.Entry::getKey)
              .collect(Collectors.toSet());

      Map<TopicPartitionReplica, Long> previousCheckedSize = new HashMap<>();

      while (!topicPartitionToTrack.isEmpty()) {
        System.out.printf(
            "[%s]%n", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        Map<TopicPartition, List<Replica>> replicaProgress =
            topicAdmin.replicas(
                topicPartitionToTrack.stream()
                    .map(TopicPartition::topic)
                    .collect(Collectors.toUnmodifiableSet()));

        Map<TopicPartition, Replica> topicPartitionLeaderReplicaTable =
            replicaProgress.entrySet().stream()
                .map(
                    x ->
                        Map.entry(
                            x.getKey(),
                            x.getValue().stream()
                                .filter(Replica::leader)
                                .findFirst()
                                .orElseThrow()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        topicPartitionToTrack.stream()
            .map(TopicPartition::topic)
            .distinct()
            .sorted()
            .forEachOrdered(
                topic -> {
                  Map<TopicPartition, List<Replica>> partitionReplicas =
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

                            System.out.printf("  │ Partition %d:%n", partition);
                            thisReplicas.stream()
                                .map(
                                    replica ->
                                        String.format(
                                            "replica on broker %3d => %s %s %s",
                                            replica.broker(),
                                            progressIndicator(replica.size(), leaderReplica.size()),
                                            dataRate(
                                                previousCheckedSize,
                                                tp,
                                                replica,
                                                leaderReplica.size()),
                                            replicaDescriptor(replica)))
                                .map(s -> String.format("  │ │ %s", s))
                                .forEachOrdered(System.out::println);
                          });
                });
        System.out.println();

        // remove synced topic-partition-replica
        Set<TopicPartition> topicPartitionFinished =
            replicaProgress.entrySet().stream()
                .filter(x -> x.getValue().stream().allMatch(Replica::inSync))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        topicPartitionToTrack.removeAll(topicPartitionFinished);

        TimeUnit.SECONDS.sleep(1);
      }

    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static String dataRate(
      Map<TopicPartitionReplica, Long> previousCheckedSize,
      TopicPartition tp,
      Replica replica,
      long leaderSize) {
    TopicPartitionReplica tpr =
        new TopicPartitionReplica(tp.topic(), tp.partition(), replica.broker());
    if (replica.leader())
      // leader don't do partition migration, so there is no data rate
      return "";
    else if (previousCheckedSize.containsKey(tpr)) {
      final long lastSize = previousCheckedSize.get(tpr);
      final long currentSize = replica.size();
      final long sizeProgress = currentSize - lastSize;

      final Duration estimatedTime =
          Duration.ofSeconds(sizeProgress == 0 ? -1 : (leaderSize - currentSize) / sizeProgress);
      final String estimated =
          estimatedTime.isNegative()
              ? "unknown"
              : estimatedTime.isZero()
                  ? "about now"
                  : Stream.of(
                          Map.entry(estimatedTime.toHoursPart(), "h"),
                          Map.entry(estimatedTime.toMinutesPart(), "m"),
                          Map.entry(estimatedTime.toSecondsPart(), "s"))
                      .dropWhile(x -> x.getKey() == 0)
                      .map(x -> x.getKey().toString() + x.getValue())
                      .collect(Collectors.joining(" ", "", " estimated"));

      // update
      previousCheckedSize.put(tpr, replica.size());

      final long TB = 1024L * 1024L * 1024L * 1024L;
      final long GB = 1024L * 1024L * 1024L;
      final long MB = 1024L * 1024L;
      final long KB = 1024L;
      if (sizeProgress > TB)
        return String.format("%.2f TB/s (%s)", (double) sizeProgress / TB, estimated);
      else if (sizeProgress > GB)
        return String.format("%.2f GB/s (%s)", (double) sizeProgress / GB, estimated);
      else if (sizeProgress > MB)
        return String.format("%.2f MB/s (%s)", (double) sizeProgress / MB, estimated);
      else if (sizeProgress > KB)
        return String.format("%.2f KB/s (%s)", (double) sizeProgress / KB, estimated);
      else return String.format("%.2f B/s  (%s)", (double) sizeProgress, estimated);
    } else {
      // update
      previousCheckedSize.put(tpr, replica.size());

      return "";
    }
  }

  static String replicaDescriptor(Replica replica) {
    return List.of(
            Optional.ofNullable(replica.leader() ? "leader" : null),
            Optional.ofNullable(replica.inSync() ? "synced" : null),
            Optional.ofNullable(replica.lag() > 0 ? "lagged" : null))
        .stream()
        .flatMap(Optional::stream)
        .collect(Collectors.joining(", ", "[", "]"));
  }

  static String progressIndicator(long current, long max) {
    double percentage = ((double) current) / max * 100.0;
    final int totalBlocks = 20;
    final int filledBlocks =
        (int) Math.min(totalBlocks, Math.floor(0.2 + percentage / (100.0 / totalBlocks)));
    final int emptyBlocks = totalBlocks - filledBlocks;

    return String.format(
        "[%s%s] %6.2f%%",
        String.join("", Collections.nCopies(filledBlocks, "#")),
        String.join("", Collections.nCopies(emptyBlocks, " ")),
        percentage);
  }

  static class Argument extends BasicArgumentWithPropFile {

    public static final String EVERY_TOPIC = "every non-synced topics...";

    @Parameter(
        names = {"--topic"},
        description = "String: topics to track",
        validateWith = ArgumentUtil.NotEmptyString.class)
    public Set<String> topics = Set.of(EVERY_TOPIC);
  }
}
