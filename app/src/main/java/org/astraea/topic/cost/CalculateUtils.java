package org.astraea.topic.cost;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;

public class CalculateUtils {
  public static Map<Integer, Map<TopicPartition, Double>> getLoad(
      Map<Integer, Map<TopicPartition, Integer>> brokerPartitionSize,
      Map<String, Integer> retentionMillis) {
    Map<Integer, Map<TopicPartition, Double>> brokerPartitionLoad = new HashMap<>();

    brokerPartitionSize
        .keySet()
        .forEach(
            (broker) -> {
              Map<TopicPartition, Double> partitionLoad =
                  brokerPartitionSize.get(broker).keySet().stream()
                      .filter(partition -> retentionMillis.containsKey(partition.topic()))
                      .collect(
                          Collectors.toMap(
                              Function.identity(),
                              partition ->
                                  (double) brokerPartitionSize.get(broker).get(partition)
                                      / retentionMillis.get(partition.topic())));
              brokerPartitionLoad.put(broker, partitionLoad);
            });
    return brokerPartitionLoad;
  }

  public static Double countSum(Set<Double> in) {
    return in.stream().mapToDouble(i -> i).sum();
  }

  public static Map<Integer, Map<TopicPartition, Double>> getScore(
      Map<Integer, Map<TopicPartition, Double>> load) {
    Map<Integer, Double> brokerLoad = new HashMap<>();
    Map<TopicPartition, Double> partitionLoad = new HashMap<>();
    Map<Integer, Double> partitionSD = new HashMap<>();
    Map<Integer, Double> partitionMean = new HashMap<>();
    Map<Integer, Map<TopicPartition, Double>> brokerPartitionScore = new HashMap<>();
    double brokerSD;
    Set<Double> loadSet = new HashSet<>();
    Set<Double> LoadSQR = new HashSet<>();

    load.keySet()
        .forEach(
            broker -> {
              load.get(broker)
                  .keySet()
                  .forEach(
                      tp -> {
                        loadSet.add(load.get(broker).get(tp));
                        partitionLoad.put(tp, load.get(broker).get(tp));
                        LoadSQR.add(Math.pow(load.get(broker).get(tp), 2));
                      });
              var loadSum = countSum(loadSet);
              var partitionNum = load.get(broker).keySet().size();
              brokerLoad.put(broker, loadSum);
              var mean = loadSum / load.get(broker).size();
              partitionMean.put(broker, loadSum / load.get(broker).size());
              var SD =
                  Math.pow((countSum(LoadSQR) - mean * mean * partitionNum) / partitionNum, 0.5);
              partitionSD.put(broker, SD);
              loadSet.clear();
              LoadSQR.clear();
            });

    brokerLoad
        .keySet()
        .forEach(
            broker -> {
              loadSet.add(brokerLoad.get(broker));
              LoadSQR.add(Math.pow(brokerLoad.get(broker), 2));
            });
    var brokerLoadMean = countSum(loadSet) / brokerLoad.keySet().size();
    var brokerLoadSQR = countSum(LoadSQR);
    brokerSD =
        Math.pow(
            (brokerLoadSQR - brokerLoadMean * brokerLoadMean * brokerLoad.keySet().size())
                / brokerLoad.keySet().size(),
            0.5);
    load.keySet()
        .forEach(
            broker -> {
              Map<TopicPartition, Double> partitionScore =
                  new TreeMap<>(
                      Comparator.comparing(TopicPartition::topic)
                          .thenComparing(TopicPartition::partition));
              load.get(broker)
                  .keySet()
                  .forEach(
                      topicPartition -> {
                        if (brokerLoad.get(broker) - brokerLoadMean > 0) {
                          partitionScore.put(
                              topicPartition,
                              Math.round(
                                      (((brokerLoad.get(broker) - brokerLoadMean) / brokerSD)
                                              * ((partitionLoad.get(topicPartition)
                                                      - partitionMean.get(broker))
                                                  / partitionSD.get(broker))
                                              * 60.0)
                                          * 100.0)
                                  / 100.0);
                        } else {
                          partitionScore.put(topicPartition, 0.0);
                        }
                        brokerPartitionScore.put(broker, partitionScore);
                      });
            });
    return brokerPartitionScore;
  }
}
