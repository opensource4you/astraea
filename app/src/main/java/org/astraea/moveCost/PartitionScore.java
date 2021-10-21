package org.astraea.moveCost;

import java.util.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicArgument;

public class PartitionScore {
  public static AdminClient client;

  static void init(String address) {
    Properties props = new Properties();
    props.put("bootstrap.servers", address); // broker's address
    props.put("acks", "-1");
    props.put("retries", 1);
    props.put("batch.size", 10);
    props.put("linger.ms", 10000);
    props.put("buffer.memory", 10240);
    client = AdminClient.create(props);
  }

  public static Map<Integer, Map<TopicPartition, Float>> getLoad()
      throws ExecutionException, InterruptedException {
    DescribeClusterResult broker = client.describeCluster();
    Collection<Integer> brokerID = new ArrayList<>();
    Map<Integer, Map<TopicPartition, Float>> data = new HashMap<>();
    float load;
    for (var j : broker.nodes().get()) {
      brokerID.add(j.id());
      DescribeLogDirsResult result = client.describeLogDirs(brokerID);
      for (var i : result.descriptions().values()) {
        try {
          Map<TopicPartition, Float> partitionLoad = new HashMap<>();
          var map = i.get();
          for (String name : map.keySet()) {
            for (var p : map.get(name).replicaInfos().keySet()) {
              if (!p.topic().equals("__consumer_offsets")) {
                load = (float) map.get(name).replicaInfos().get(p).size() / getRetention(p.topic());
                partitionLoad.put(p, load);
              }
            }
          }
          data.put(j.id(), partitionLoad);
        } catch (InterruptedException | ExecutionException e) {
          System.out.println("取得資訊失敗" + e.getMessage());
          e.printStackTrace();
        }
      }
      brokerID.remove(j.id());
    }
    System.out.println();
    System.out.println();
    return data;
  }

  public static Map<Integer, Map<TopicPartition, Float>> getScore()
      throws ExecutionException, InterruptedException {
    Map<Integer, Map<TopicPartition, Float>> data = getLoad();
    Map<Integer, Float> brokerLoad = new HashMap<>();
    Map<TopicPartition, Float> partitionLoad = new HashMap<>();
    Map<Integer, Float> partitionSD = new HashMap<>();
    Map<Integer, Float> partitionMean = new HashMap<>();
    Map<Integer, Map<TopicPartition, Float>> score = new HashMap<>();

    float mean = 0f, LoadSQR = 0f, SD, brokerSD;
    int partitionNum;
    for (var broker : data.keySet()) {
      for (var topicPartition : data.get(broker).keySet()) {
        mean += data.get(broker).get(topicPartition);
        partitionLoad.put(topicPartition, data.get(broker).get(topicPartition));
        LoadSQR += (float) Math.pow(data.get(broker).get(topicPartition), 2);
      }
      partitionNum = data.get(broker).keySet().size();
      brokerLoad.put(broker, mean);
      mean /= partitionNum;
      partitionMean.put(broker, mean);
      SD = (float) Math.pow((LoadSQR - mean * mean * partitionNum) / partitionNum, 0.5);
      partitionSD.put(broker, SD);
      mean = 0f;
      LoadSQR = 0f;
    }
    for (var i : brokerLoad.keySet()) {
      mean += brokerLoad.get(i);
      LoadSQR += (float) Math.pow(brokerLoad.get(i), 2);
    }
    mean /= brokerLoad.keySet().size();
    brokerSD =
        (float)
            Math.pow(
                (LoadSQR - mean * mean * brokerLoad.keySet().size()) / brokerLoad.keySet().size(),
                0.5);
    for (var broker : data.keySet()) {
      Map<TopicPartition, Float> partitionScore = new HashMap<>();
      for (var ii : data.get(broker).keySet()) {
        if (brokerLoad.get(broker) - mean > 0) {
          partitionScore.put(
              ii,
              ((brokerLoad.get(broker) - mean) / brokerSD)
                  * ((partitionLoad.get(ii) - partitionMean.get(broker)) / partitionSD.get(broker))
                  * 60);
          score.put(broker, partitionScore);

        } else {
          partitionScore.put(ii, 0f);
          score.put(broker, partitionScore);
        }
      }
    }
    return score;
  }

  public static int getRetention(String topic) {
    int retentionTime = -1;
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    try {
      retentionTime =
          Integer.parseInt(
              client
                  .describeConfigs(List.of(configResource))
                  .all()
                  .get()
                  .get(configResource)
                  .get("retention.ms")
                  .value());
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return retentionTime;
  }

  public static void printScore(Map<Integer, Map<TopicPartition, Float>> score) {
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
    init(argument.brokers);
    Map<Integer, Map<TopicPartition, Float>> score = getScore();
    printScore(score);
  }

  static class Argument extends BasicArgument {}
}
