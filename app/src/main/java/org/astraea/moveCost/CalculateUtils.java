package org.astraea.moveCost;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;

public class CalculateUtils {
  public static Map<Integer, Map<TopicPartition, Double>> getLoad(
      Map<Integer, Map<TopicPartition, Integer>> broker_partitionSize,
      Map<String, Integer> retention_ms) {
    Map<Integer, Map<TopicPartition, Double>> broker_partitionLoad = new HashMap<>();
    double load;
    for (var broker : broker_partitionSize.keySet()) {
      Map<TopicPartition, Double> partitionLoad = new HashMap<>();
      for (var partition : broker_partitionSize.get(broker).keySet()) {
        load =
            (double) broker_partitionSize.get(broker).get(partition)
                / retention_ms.get(partition.topic());
        partitionLoad.put(partition, load);
      }
      broker_partitionLoad.put(broker, partitionLoad);
    }
    return broker_partitionLoad;
  }

  public static Map<Integer, Map<TopicPartition, Double>> getScore(
      Map<Integer, Map<TopicPartition, Double>> load) {
    Map<Integer, Double> brokerLoad = new HashMap<>();
    Map<TopicPartition, Double> partitionLoad = new HashMap<>();
    Map<Integer, Double> partitionSD = new HashMap<>();
    Map<Integer, Double> partitionMean = new HashMap<>();
    Map<Integer, Map<TopicPartition, Double>> broker_partitionScore = new HashMap<>();

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
      Map<TopicPartition, Double> partitionScore = new HashMap<>();
      for (var topicPartition : load.get(broker).keySet()) {
        if (brokerLoad.get(broker) - mean > 0) {
          partitionScore.put(
              topicPartition,
              ((brokerLoad.get(broker) - mean) / brokerSD)
                  * ((partitionLoad.get(topicPartition) - partitionMean.get(broker))
                      / partitionSD.get(broker))
                  * 60.0);

        } else {
          partitionScore.put(topicPartition, 0.0);
        }
        broker_partitionScore.put(broker, partitionScore);
      }
    }
    return broker_partitionScore;
  }
}
