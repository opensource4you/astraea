package org.astraea.cost;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.admin.BeansGetter;
import org.astraea.admin.TopicPartition;

public class ClusterInfoProvider {

  public static ClusterInfo fakeClusterInfo(
      int nodeCount, int topicCount, int partitionCount, int replicaCount) {
    final var random = new Random();
    random.setSeed(0);

    return fakeClusterInfo(
        nodeCount,
        topicCount,
        partitionCount,
        replicaCount,
        (partition) ->
            IntStream.range(0, partition)
                .mapToObj(
                    (ignore) -> {
                      final var suffix =
                          IntStream.range(0, 5)
                              .map(i -> random.nextInt(26))
                              .map(i -> 'a' + i)
                              .mapToObj(i -> String.valueOf((char) i))
                              .collect(Collectors.joining());
                      return "fake-topic-" + suffix;
                    })
                .collect(Collectors.toUnmodifiableSet()));
  }

  public static ClusterInfo fakeClusterInfo(
      int nodeCount,
      int topicCount,
      int partitionCount,
      int replicaCount,
      Function<Integer, Set<String>> topicNameGenerator) {
    final var nodes =
        IntStream.range(0, nodeCount)
            .mapToObj(nodeId -> NodeInfo.of(nodeId, "host" + nodeId, 9092))
            .collect(Collectors.toUnmodifiableList());
    final var dataDirCount = 3;
    final var dataDirectories =
        IntStream.range(0, dataDirCount)
            .mapToObj(i -> "/tmp/data-directory-" + i)
            .collect(Collectors.toUnmodifiableSet());
    final var dataDirectoryList = List.copyOf(dataDirectories);
    final var topics = topicNameGenerator.apply(topicCount);
    final var replicas =
        topics.stream()
            .flatMap(
                topic ->
                    IntStream.range(0, partitionCount)
                        .mapToObj(p -> TopicPartition.of(topic, Integer.toString(p))))
            .flatMap(
                tp ->
                    IntStream.range(0, replicaCount)
                        .mapToObj(
                            r ->
                                ReplicaInfo.of(
                                    tp.topic(),
                                    tp.partition(),
                                    nodes.get(r),
                                    r == 0,
                                    true,
                                    false,
                                    dataDirectoryList.get(
                                        tp.partition() % dataDirectories.size()))))
            .collect(Collectors.groupingBy(ReplicaInfo::topic));

    return new ClusterInfo() {
      @Override
      public List<NodeInfo> nodes() {
        return nodes;
      }

      @Override
      public Set<String> dataDirectories(int brokerId) {
        return dataDirectories;
      }

      @Override
      public List<ReplicaInfo> availableReplicaLeaders(String topic) {
        return replicas(topic).stream()
            .filter(ReplicaInfo::isLeader)
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public List<ReplicaInfo> availableReplicas(String topic) {
        return replicas(topic);
      }

      @Override
      public Set<String> topics() {
        return topics;
      }

      @Override
      public List<ReplicaInfo> replicas(String topic) {
        return replicas.get(topic);
      }

      @Override
      public BeansGetter beans() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
