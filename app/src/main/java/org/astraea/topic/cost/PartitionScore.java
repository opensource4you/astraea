package org.astraea.topic.cost;

import java.util.*;
import org.apache.kafka.common.TopicPartition;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicArgument;
import org.astraea.topic.TopicAdmin;

public class PartitionScore {
  TopicAdmin admin;
  Map<Integer, Map<TopicPartition, Integer>> brokerPartitionSize;
  Map<Integer, Map<TopicPartition, Double>> score;
  Map<Integer, Map<TopicPartition, Double>> load;
  Map<String, Integer> retentionMillis;

  public PartitionScore(String address) {
    admin = TopicAdmin.of(address);
    brokerPartitionSize = GetPartitionInf.getSize(admin);
    retentionMillis = GetPartitionInf.getRetentionMillis(admin);
    load = CalculateUtils.getLoad(brokerPartitionSize, retentionMillis);
    score = CalculateUtils.getScore(load);
  }

  public void printScore(Map<Integer, Map<TopicPartition, Double>> score) {
    Map<Integer, Boolean> brokerGood = new HashMap();
    for (var broker : score.keySet()) {
      brokerGood.put(broker, true);
      for (var tp : score.get(broker).keySet())
        if (score.get(broker).get(tp) > 0) brokerGood.put(broker, false);
      if (brokerGood.get(broker)) {
        System.out.println();
        System.out.println("broker: " + broker + " is balance");
      } else {
        System.out.println();
        System.out.println("broker: " + broker);
        for (var tp : score.get(broker).keySet()) {
          if (score.get(broker).get(tp) > 0) {
            System.out.println("topic-partition: " + tp);
            System.out.println("score: " + score.get(broker).get(tp));
          } else {
            System.out.println("topic-partition: " + tp + " is balance");
          }
        }
      }
    }
  }

  public static void main(String[] args) {
    var argument = ArgumentUtil.parseArgument(new Argument(), args);
    var partitionScore = new PartitionScore(argument.brokers);
    partitionScore.printScore(partitionScore.score);
  }

  static class Argument extends BasicArgument {}
}
