package org.astraea.balancer.alpha.generator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.balancer.alpha.ClusterLogAllocation;
import org.astraea.balancer.alpha.RebalancePlanProposal;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.NodeInfo;

public class ShufflePlanGenerator implements RebalancePlanGenerator {

  private final Supplier<Integer> numberOfShuffle;

  public ShufflePlanGenerator(int origin, int bound) {
    this(() -> ThreadLocalRandom.current().nextInt(origin, bound));
  }

  public ShufflePlanGenerator(Supplier<Integer> numberOfShuffle) {
    this.numberOfShuffle = numberOfShuffle;
  }

  @Override
  public RebalancePlanProposal generate(ClusterInfo clusterNow) {
    List<NodeInfo> nodes = clusterNow.nodes();

    Map<TopicPartition, NodeInfo> tpLeader =
        clusterNow.topics().stream()
            .flatMap(topic -> clusterNow.partitions(topic).stream())
            .map(
                pInfo ->
                    Map.entry(new TopicPartition(pInfo.topic(), pInfo.partition()), pInfo.leader()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    List<TopicPartitionReplica> allReplicas =
        clusterNow.topics().stream()
            .map(topic -> Map.entry(topic, clusterNow.partitions(topic)))
            .flatMap(
                entry ->
                    entry.getValue().stream()
                        .flatMap(
                            partition ->
                                partition.replicas().stream()
                                    .map(
                                        replica ->
                                            new TopicPartitionReplica(
                                                entry.getKey(),
                                                partition.partition(),
                                                replica.id()))))
            .collect(Collectors.toList());

    if (allReplicas.isEmpty()) {
      return RebalancePlanProposal.builder()
          .noRebalancePlan()
          .addInfo(List.of("This cluster has no topic or partition."))
          .build();
    }

    // make some shuffles, randomly change the located broker of replicas.
    int shuffles = numberOfShuffle.get();
    var shuffleRecord =
        new TreeSet<>(
            Comparator.comparing((Movement m) -> m.topic)
                .thenComparing((Movement m) -> m.partition)
                .thenComparing((Movement m) -> m.sourceBrokerId)
                .thenComparing((Movement m) -> m.destinationBrokerId));
    IntStream.range(0, shuffles)
        .map(x -> ThreadLocalRandom.current().nextInt(allReplicas.size()))
        .forEach(
            replicaIndex -> {
              var target = allReplicas.get(replicaIndex);
              var randomNode = nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));
              var topicPartition = new TopicPartition(target.topic(), target.partition());
              var replacement =
                  new TopicPartitionReplica(target.topic(), target.partition(), randomNode.id());
              allReplicas.set(replicaIndex, replacement);

              shuffleRecord.add(
                  new Movement(
                      target.topic(), target.partition(), target.brokerId(), randomNode.id()));

              // if this replica belongs to the leader, we need to update tpLeader map
              if (tpLeader.get(topicPartition).id() == randomNode.id())
                tpLeader.put(topicPartition, randomNode);
            });

    // wrap the result
    Map<String, Map<Integer, List<Integer>>> collect =
        allReplicas.stream()
            .collect(
                Collectors.groupingBy(
                    TopicPartitionReplica::topic,
                    Collectors.groupingBy(
                        TopicPartitionReplica::partition,
                        Collectors.mapping(TopicPartitionReplica::brokerId, Collectors.toList()))));

    // sort the inner replica
    collect.forEach(
        (topic, partitionReplica) ->
            partitionReplica.forEach(
                (partition, replicaList) ->
                    replicaList.sort(
                        Comparator.comparing(
                            x -> x == clusterNow.partitions(topic).get(partition).leader().id()))));

    return RebalancePlanProposal.builder()
        .withRebalancePlan(new ClusterLogAllocation(collect))
        .addInfo(List.of("Make " + shuffles + " replica shuffle in this cluster."))
        .addInfo(
            shuffleRecord.stream()
                .map(
                    x ->
                        String.format(
                            "move topic %10s partition %6d, from %6d to %6d.",
                            x.topic, x.partition, x.sourceBrokerId, x.destinationBrokerId))
                .collect(Collectors.toUnmodifiableList()))
        .build();
  }

  private static class Movement {
    private final String topic;
    private final int partition;
    private final int sourceBrokerId;
    private final int destinationBrokerId;

    private Movement(String topic, int partition, int sourceBrokerId, int destinationBrokerId) {
      this.topic = topic;
      this.partition = partition;
      this.sourceBrokerId = sourceBrokerId;
      this.destinationBrokerId = destinationBrokerId;
    }
  }
}
