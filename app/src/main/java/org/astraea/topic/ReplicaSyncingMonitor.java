package org.astraea.topic;

import com.beust.jcommander.Parameter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicArgument;
import org.astraea.argument.BasicArgumentWithPropFile;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ReplicaSyncingMonitor {

    public static void main(String[] args) {
        execute(ArgumentUtil.parseArgument(new Argument(), args));
    }

    static void execute(final Argument argument) {
        try(TopicAdmin topicAdmin = TopicAdmin.of(argument.props())){

            Set<String> topicToTrack = argument.topics.contains(Argument.EVERY_TOPIC) ?
                    topicAdmin.topicNames() :
                    argument.topics;

            Set<TopicPartition> topicPartitionToTrack = topicAdmin.replicas(topicToTrack)
                        .entrySet()
                        .stream()
                        .filter(tpr -> tpr.getValue().stream().anyMatch(replica -> !replica.inSync()))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toSet());

            while(!topicPartitionToTrack.isEmpty()) {
                System.out.printf("[%s]%n", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                Map<TopicPartition, List<Replica>> replicaProgress = topicAdmin.replicas(topicPartitionToTrack
                        .stream()
                        .map(TopicPartition::topic)
                        .collect(Collectors.toUnmodifiableSet()));

                Map<TopicPartition, Replica> topicPartitionLeaderReplicaTable = replicaProgress.entrySet().stream()
                        .map(x -> Map.entry(x.getKey(), x.getValue().stream().filter(Replica::leader).findFirst().orElseThrow()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                topicPartitionToTrack.stream()
                        .map(TopicPartition::topic)
                        .distinct()
                        .sorted()
                        .forEachOrdered(topic -> {
                            Map<TopicPartition, List<Replica>> partitionReplicas = replicaProgress.entrySet().stream()
                                    .filter(tpr -> tpr.getKey().topic().equals(topic))
                                    .filter(tpr -> topicPartitionToTrack.contains(tpr.getKey()))
                                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                            System.out.printf("  Topic \"%s\":%n", topic);

                            partitionReplicas.keySet().stream()
                                    .map(TopicPartition::partition)
                                    .distinct()
                                    .sorted()
                                    .forEachOrdered(partition -> {
                                        TopicPartition tp = new TopicPartition(topic, partition);
                                        Replica leaderReplica = topicPartitionLeaderReplicaTable.get(tp);
                                        List<Replica> thisReplicas = partitionReplicas.get(tp);

                                        System.out.printf("  │ Partition %d:%n", partition);
                                        thisReplicas.stream()
                                                .map(replica -> String.format("replica on broker %3d => %s %s",
                                                        replica.broker(),
                                                        progressIndicator(replica.size(), leaderReplica.size()),
                                                        replicaDescriptor(replica)))
                                                .map(s -> String.format("  │ │ %s", s))
                                                .forEachOrdered(System.out::println);
                                    });
                        });
                System.out.println();

                // remove synced topic-partition-replica
                Set<TopicPartition> topicPartitionFinished = replicaProgress.entrySet().stream()
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

    static String replicaDescriptor(Replica replica) {
        return List.of(
                Optional.ofNullable(replica.leader() ? "leader" : null),
                Optional.ofNullable(replica.inSync() ? "synced" : null),
                Optional.ofNullable(replica.lag() > 0 ? "lagged" : null)
                )
                .stream()
                .flatMap(Optional::stream)
                .collect(Collectors.joining(", ", "[", "]"));
    }

    static String progressIndicator(long current, long max) {
        double percentage = ((double)current) / max * 100.0;
        final int totalBlocks = 20;
        final int filledBlocks = (int)Math.min(totalBlocks, Math.floor(0.2 + percentage / (100.0 / totalBlocks)));
        final int emptyBlocks = totalBlocks - filledBlocks;

        return String.format("[%s%s] %6.2f%%",
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
