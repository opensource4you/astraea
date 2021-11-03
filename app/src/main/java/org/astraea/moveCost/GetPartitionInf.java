package org.astraea.moveCost;

import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;

public class GetPartitionInf {
  static Map<Integer, Map<TopicPartition, Integer>> getSize(AdminClient client)
      throws ExecutionException, InterruptedException {
    var broker = client.describeCluster();
    Collection<Integer> brokerID = new ArrayList<>();
    Map<Integer, Map<TopicPartition, Integer>> broker_partitionSize = new HashMap<>();
    for (var j : broker.nodes().get()) {
      Map<TopicPartition, Integer> partitionSize = new HashMap<>();
      brokerID.add(j.id());
      var result = client.describeLogDirs(brokerID);
      for (var i : result.descriptions().values()) {
        try {
          var map = i.get();
          for (String name : map.keySet()) {
            System.out.println("成功取得broker" + j + "(" + name + "): ");
            System.out.println(name + ": " + map.get(name));
            for (var p : map.get(name).replicaInfos().keySet()) {
              partitionSize.put(p, (int) map.get(name).replicaInfos().get(p).size());
              System.out.println("topic-partition: " + p);
              System.out.println("size: " + map.get(name).replicaInfos().get(p).size());
            }
          }
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
      }
      brokerID.remove(j.id());
      broker_partitionSize.put(j.id(), partitionSize);
    }
    return broker_partitionSize;
  }

  static Map<String, Integer> getRetention_ms(AdminClient client)
      throws ExecutionException, InterruptedException {
    Map<String, Integer> retentionTime = new HashMap<>();
    for (var topic : client.listTopics(new ListTopicsOptions().listInternal(true)).names().get()) {
      ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
      try {
        retentionTime.put(
            topic,
            Integer.parseInt(
                client
                    .describeConfigs(List.of(configResource))
                    .all()
                    .get()
                    .get(configResource)
                    .get("retention.ms")
                    .value()));
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
    return retentionTime;
  }
}
