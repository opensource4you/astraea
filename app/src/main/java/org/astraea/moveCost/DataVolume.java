package org.astraea.moveCost;
import java.util.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;

public class DataVolume {
  public static AdminClient client;

  static synchronized Producer initProducer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "192.168.103.39:9092"); // *borker's ip
    props.put("acks", "-1");
    props.put("retries", 1);
    props.put("batch.size", 10);
    props.put("linger.ms", 10000);
    props.put("buffer.memory", 10240);
    client = AdminClient.create(props);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    Producer producer = new KafkaProducer(props);
    return producer;
  }

  public static Map<Integer, Map<TopicPartition, Float>> getLoad()
      throws ExecutionException, InterruptedException {
    ListTopicsResult topic = client.listTopics();
    KafkaFuture<Set<String>> names = topic.names();
    DescribeClusterResult broker = client.describeCluster();
    Collection<Integer> brokerID = new ArrayList<Integer>();
    Map<Integer, Map<TopicPartition, Float>> data = new HashMap<>();
    Float load = 0f;
    for (var j : broker.nodes().get()) {
      brokerID.add(j.id());
      DescribeLogDirsResult result = client.describeLogDirs(brokerID);
      for (var i : result.descriptions().values()) {
        try {
          Map<TopicPartition, Float> partitionLoad = new HashMap<>();
          var map = i.get();
          for (String name : map.keySet()) {
            System.out.println("成功取得broker" + j.id() + "(" + name + "): ");
            System.out.println(name + ": " + map.get(name));
            for (var p : map.get(name).replicaInfos().keySet()) {
              if (!p.topic().equals("__consumer_offsets")) {
                System.out.println("topic-partition: " + p);
                System.out.println("size: " + map.get(name).replicaInfos().get(p).size());
                //     System.out.println("retention.ms:  "+getRetention(p.topic()));
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

    Float mean = 0f, tmp = 0f, LoadSQR = 0f, SD, brokerSD;
    int partitionNum = 0;
    for (var broker : data.keySet()) {
      for (var ii : data.get(broker).keySet()) {
        mean += data.get(broker).get(ii);
        // System.out.println("broker: "+broker +" topic-partition: "+ ii+ "load: " +
        partitionLoad.put(ii, data.get(broker).get(ii));
        LoadSQR += (float) Math.pow(data.get(broker).get(ii), 2);
      }
      partitionNum = data.get(broker).keySet().size();
      // System.out.println("brokerload: "+mean);
      brokerLoad.put(broker, mean);
      mean /= partitionNum;
      // System.out.println("partitionMean: "+mean);
      partitionMean.put(broker, mean);
      SD = (float) Math.pow((LoadSQR - mean * mean * partitionNum) / partitionNum, 0.5);
      // System.out.println("partitionSD= "+SD);
      partitionSD.put(broker, SD);
      mean = 0f;
      LoadSQR = 0f;
    }
    for (var i : brokerLoad.keySet()) {
      //  System.out.println(i+" "+brokerLoad.get(i));
      mean += brokerLoad.get(i);
      LoadSQR += (float) Math.pow(brokerLoad.get(i), 2);
    }
    mean /= brokerLoad.keySet().size();
    //  System.out.println("brokerLoad mean= "+mean);
    brokerSD =
        (float)
            Math.pow(
                (LoadSQR - mean * mean * brokerLoad.keySet().size()) / brokerLoad.keySet().size(),
                0.5);
    //   System.out.println("brokerSD= "+brokerSD);
    for (var broker : data.keySet()) {
      //  System.out.println("broker: "+broker);
      Map<TopicPartition, Float> partitionScore = new HashMap<>();
      for (var ii : data.get(broker).keySet()) {
        //  System.out.println("topic-partition: "+ii);
        if (brokerLoad.get(broker) - mean > 0) {
          partitionScore.put(
              ii,
              ((brokerLoad.get(broker) - mean) / brokerSD)
                  * ((partitionLoad.get(ii) - partitionMean.get(broker)) / partitionSD.get(broker))
                  * 60);
          score.put(broker, partitionScore);

        } else {
          //  System.out.println("broker: "+broker +" is good.");
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
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
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
    initProducer();
    Map<Integer, Map<TopicPartition, Float>> score = new HashMap<>();
    score = getScore();
    printScore(score);
  }
}
