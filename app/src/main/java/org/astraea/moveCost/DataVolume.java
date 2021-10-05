package org.astraea.moveCost;

import java.util.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;

public class DataVolume {
  public static AdminClient client;

  static synchronized Producer initProducer() {
    Properties props = new Properties();
    // AdminClient client=new AdminClient();
    // 設定kafka叢集的地址
    props.put("bootstrap.servers", "192.168.103.39:9092"); // *borker's ip
    // ack模式，all是最慢但最安全的
    props.put("acks", "-1");
    // 失敗重試次數
    props.put("retries", 1);
    // 每個分割區未傳送訊息總位元組大小（單位：位元組），超過設定的值就會提交資料到伺服器端
    props.put("batch.size", 10);
    // props.put("max.request.size",10);
    // 訊息在緩衝區保留的時間，超過設定的值就會被提交到伺服器端
    props.put("linger.ms", 10000);
    // 整個Producer用到總記憶體的大小，如果緩衝區滿了會提交資料到伺服器端
    // buffer.memory要大於batch.size，否則會報申請記憶體不足的錯誤
    props.put("buffer.memory", 10240);
    // 序列化器
    client = AdminClient.create(props);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    Producer producer = new KafkaProducer(props);
    return producer;
  }

  public static Set getTopicNum() {
    // list all topics
    ListTopicsResult result = client.listTopics();
    KafkaFuture<Set<String>> names = result.names();
    try {
      Set<String> set = names.get();
      System.out.println("topic name: ");
      set.forEach(System.out::println);
      return (set);
    } catch (InterruptedException | ExecutionException e) {
      System.out.println("取得topic name失敗" + e.getMessage());
      e.printStackTrace();
    }
    System.out.println("");
    return null;
  }

  public static void getSize() {
    /*
    TopicPartitionReplica topicPartitionReplica = new TopicPartitionReplica(topic, 1, 0);
    Collection<TopicPartitionReplica> replicaAssignment = new ArrayList<TopicPartitionReplica>();
    replicaAssignment.add(topicPartitionReplica);
    System.out.println(replicaAssignment.size());
     */
    Set brokerName = getTopicNum();
    Collection<Integer> brokerID = new ArrayList<Integer>();
    for (int j = 0; j <= brokerName.size() - 1; j++) {
      brokerID.add(j);
      DescribeLogDirsResult result = client.describeLogDirs(brokerID);
      for (var i : result.descriptions().values()) {
        try {
          var map = i.get();
          for (String name : map.keySet()) {
            System.out.println("成功取得broker" +j + ": ");
            System.out.println(name + ": " + map.get(name));
          }
        } catch (InterruptedException | ExecutionException e) {
          System.out.println("取得資訊失敗" + e.getMessage());
          e.printStackTrace();
        }
      }
      brokerID.remove(j);
    }
  }
  public static void alterPartitionReassignment(String topic, Integer p,List<Integer> list) {
    try {
      TopicPartition topicPartition = new TopicPartition(topic, p);
      Map<TopicPartition,Optional<NewPartitionReassignment>> replicaAssignment= new HashMap<>();
      replicaAssignment.put(topicPartition,Optional.of(new NewPartitionReassignment(list)));
      System.out.println("開始移動partition");
      AlterPartitionReassignmentsResult result = client.alterPartitionReassignments(replicaAssignment);
      result.all().get();
      System.out.println("Partition移動成功！");

    } catch (InterruptedException e) {
      System.out.println("移動失敗：" + e.getMessage());
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }


  public static void alterReplicaLogDir(String topic, Integer p, String path) {
    try {
      TopicPartitionReplica topicPartitionReplica = new TopicPartitionReplica(topic, p, 0);
      Map<TopicPartitionReplica, String> replicaAssignment =
          new HashMap<TopicPartitionReplica, String>();
      replicaAssignment.put(topicPartitionReplica, path); // path必須在server.properties中的log.dirs
      System.out.println("開始移動log文件");

      AlterReplicaLogDirsResult result = client.alterReplicaLogDirs(replicaAssignment);
      result.all().get();
      System.out.println("Topic移動文件夹成功！");

    } catch (InterruptedException e) {
      System.out.println("移動文件失敗：" + e.getMessage());
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  public static void testPrint(Producer producer,String topic) {
    for (int i = 0; i < 10; i++)
      producer.send(
          new ProducerRecord<String, String>(topic, Integer.toString(i), "test訊息:" + i),
          new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
              System.out.println("訊息傳送狀態監測");
            }
          });
    producer.close();
  }

  public static void main(String[] args) throws InterruptedException {
    Producer producer = initProducer();
    // 搬移partition log
    // alterReplicaLogDir("test0", 1, "/home/sean/Document/test0/log1");
    // 搬移partition至不同的broker
     //alterPartitionReassignment("test0",0,List.of(2));
    testPrint(producer,"test2"); // 塞東西給producer 取得到的logSize才不會是0
    getSize(); // 取得所有partition的名稱與大小等資訊
  }
}
