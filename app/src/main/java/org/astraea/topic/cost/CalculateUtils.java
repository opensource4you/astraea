package org.astraea.topic.cost;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.kafka.common.TopicPartition;

public class CalculateUtils {
  public static Map<Integer, Map<TopicPartition, Double>> getLoad(
      Map<Integer, Map<TopicPartition, Integer>> brokerPartitionSize,
      Map<String, Integer> retentionMillis) {
    Map<Integer, Map<TopicPartition, Double>> brokerPartitionLoad = new HashMap<>();
    double load;
    for (var broker : brokerPartitionSize.keySet()) {
      Map<TopicPartition, Double> partitionLoad = new HashMap<>();
      for (var partition : brokerPartitionSize.get(broker).keySet()) {
        if (retentionMillis.containsKey(partition.topic())) {
          load =
              (double) brokerPartitionSize.get(broker).get(partition)
                  / retentionMillis.get(partition.topic());
          partitionLoad.put(partition, load);
        }
      }
      brokerPartitionLoad.put(broker, partitionLoad);
    }
    return brokerPartitionLoad;
  }

  public static Map<Integer, Map<TopicPartition, Double>> getScore(
      Map<Integer, Map<TopicPartition, Double>> load) {
    Map<Integer, Double> brokerLoad = new HashMap<>();
    Map<TopicPartition, Double> partitionLoad = new HashMap<>();
    Map<Integer, Double> partitionSD = new HashMap<>();
    Map<Integer, Double> partitionMean = new HashMap<>();
    Map<Integer, Map<TopicPartition, Double>> brokerPartitionScore = new HashMap<>();

    double mean = 0f, LoadSQR = 0f, SD, brokerSD;
    int partitionNum;
    for (var broker : load.keySet()) {
      for (var topicPartition : load.get(broker).keySet()) {
        mean += load.get(broker).get(topicPartition);
        partitionLoad.put(topicPartition, load.get(broker).get(topicPartition));
        LoadSQR += Math.pow(load.get(broker).get(topicPartition), 2);
      }
      partitionNum = load.get(broker).keySet().size();
      brokerLoad.put(broker, mean);
      mean /= partitionNum;
      partitionMean.put(broker, mean);
      SD = Math.pow((LoadSQR - mean * mean * partitionNum) / partitionNum, 0.5);
      partitionSD.put(broker, SD);
      mean = 0f;
      LoadSQR = 0f;
    }

    for (var i : brokerLoad.keySet()) {
      mean += brokerLoad.get(i);
      LoadSQR += Math.pow(brokerLoad.get(i), 2);
    }
    mean /= brokerLoad.keySet().size();
    brokerSD =
        Math.pow(
            (LoadSQR - mean * mean * brokerLoad.keySet().size()) / brokerLoad.keySet().size(), 0.5);
    for (var broker : load.keySet()) {
      Map<TopicPartition, Double> partitionScore =
          new TreeMap<>(
              Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
      for (var topicPartition : load.get(broker).keySet()) {
        if (brokerLoad.get(broker) - mean > 0) {
          partitionScore.put(
              topicPartition,
              Math.round(
                      (((brokerLoad.get(broker) - mean) / brokerSD)
                              * ((partitionLoad.get(topicPartition) - partitionMean.get(broker))
                                  / partitionSD.get(broker))
                              * 60.0)
                          * 100.0)
                  / 100.0);
        } else {
          partitionScore.put(topicPartition, 0.0);
        }
        brokerPartitionScore.put(broker, partitionScore);
      }
    }
    return brokerPartitionScore;
  }
}
