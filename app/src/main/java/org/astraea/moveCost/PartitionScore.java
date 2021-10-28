package org.astraea.moveCost;

import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicArgument;

public class PartitionScore {
  AdminClient client;
  Map<Integer, Map<TopicPartition, Integer>> broker_partitionSize;
  Map<Integer, Map<TopicPartition, Double>> score;
  Map<Integer, Map<TopicPartition, Double>> load;
  Map<String, Integer> retention_ms;

  public PartitionScore(String address) throws ExecutionException, InterruptedException {
    init(address);
    broker_partitionSize = GetPartitionInf.getSize(client);
    retention_ms = GetPartitionInf.getRetention_ms(client);
    load = CalculateUtils.getLoad(broker_partitionSize, retention_ms);
    score = CalculateUtils.getScore(load);
  }

  void init(String address) {
    var props = new Properties();
    props.put("bootstrap.servers", address); // broker's address
    props.put("acks", "-1");
    props.put("retries", 1);
    props.put("batch.size", 10);
    props.put("linger.ms", 10000);
    props.put("buffer.memory", 10240);
    client = AdminClient.create(props);
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

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    var argument = ArgumentUtil.parseArgument(new Argument(), args);
    var partitionScore = new PartitionScore(argument.brokers);
    partitionScore.printScore(partitionScore.score);
  }

  static class Argument extends BasicArgument {}
}
