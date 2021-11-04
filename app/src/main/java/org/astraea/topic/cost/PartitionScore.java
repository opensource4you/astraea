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
    for (var i : score.keySet()) {
      System.out.println("broker: " + i);
      for (var j : score.get(i).keySet()) {
        System.out.println("topic-partition: " + j);
        System.out.println("score: " + score.get(i).get(j));
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
