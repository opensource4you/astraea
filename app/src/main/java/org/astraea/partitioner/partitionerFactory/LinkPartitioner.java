package org.astraea.partitioner.partitionerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.astraea.concurrent.ThreadPool;
import org.astraea.partitioner.nodeLoadMetric.BrokersWeight;
import org.astraea.partitioner.nodeLoadMetric.LoadPoisson;
import org.astraea.partitioner.nodeLoadMetric.NodeLoadClient;
import org.astraea.topic.TopicAdmin;

public class LinkPartitioner implements Partitioner {

  private static final SmoothPartitionerFactory FACTORY =
      new SmoothPartitionerFactory(
          Comparator.comparing(o -> o.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).toString()));

  private Partitioner partitioner;

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    return partitioner.partition(topic, key, keyBytes, value, valueBytes, cluster);
  }

  @Override
  public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
    partitioner.onNewBatch(topic, cluster, prevPartition);
  }

  @Override
  public void close() {
    partitioner.close();
  }

  @Override
  public void configure(Map<String, ?> configs) throws NullPointerException {
    Objects.requireNonNull(
        (String) configs.get("jmx_servers"), "You must configure jmx_servers correctly");
    partitioner = FACTORY.getOrCreate(ThreadSafeSmoothPartitioner.class, configs);
  }

  public Partitioner getPartitioner() {
    return partitioner;
  }

  public static class ThreadSafeSmoothPartitioner implements Partitioner {
    private NodeLoadClient nodeLoadClient;
    private ThreadPool pool;
    private String bootstrapServers;
    private HashMap<String, String> addressMap = new HashMap<>();
    private Set<String> existPartitionsNodeID = new HashSet<>();

    ThreadSafeSmoothPartitioner() {}

    @Override
    public int partition(
        String topic,
        Object key,
        byte[] keyBytes,
        Object value,
        byte[] valueBytes,
        Cluster cluster) {
      LoadPoisson loadPoisson = new LoadPoisson(nodeLoadClient);
      BrokersWeight brokersWeight = new BrokersWeight(loadPoisson);
      brokersWeight.setBrokerHashMap(existPartitionsNodeID);
      Map.Entry<String, int[]> maxWeightServer = null;

      int allWeight = brokersWeight.getAllWeight();
      HashMap<String, int[]> currentBrokerHashMap = brokersWeight.getBrokerHashMap();
      for (Map.Entry<String, int[]> item : currentBrokerHashMap.entrySet()) {
        if (maxWeightServer == null || item.getValue()[1] > maxWeightServer.getValue()[1]) {
          maxWeightServer = item;
        }
        currentBrokerHashMap.put(
            item.getKey(), new int[] {item.getValue()[0], item.getValue()[1] + item.getValue()[0]});
      }
      assert maxWeightServer != null;
      currentBrokerHashMap.put(
          maxWeightServer.getKey(),
          new int[] {maxWeightServer.getValue()[0], maxWeightServer.getValue()[1] - allWeight});

      ArrayList<Integer> partitionList = new ArrayList<>();
      for (PartitionInfo partitionInfo :
          cluster.partitionsForNode(Integer.parseInt(maxWeightServer.getKey()))) {
        partitionList.add(partitionInfo.partition());
      }

      // When assigned to a broker without partition.
      if (partitionList.size() == 0) {
        try (var topicAdmin = TopicAdmin.of(bootstrapServers)) {
          var topicNameSet = new HashSet<String>();
          topicNameSet.add(topic);
          var leaders = new HashSet<String>();
          topicAdmin.leaders(topicNameSet).entrySet().stream()
              .map(Map.Entry::getValue)
              .forEach(s -> leaders.add(s.toString()));
          var newBrokerHashMap = new HashMap<String, int[]>();

          leaders.forEach(s -> newBrokerHashMap.put(s, new int[] {0, 0}));

          for (Map.Entry<String, int[]> broker : newBrokerHashMap.entrySet()) {
            var k = broker.getKey();
            var nullJmxBroker = 1;

            for (Map.Entry<String, String> adr : addressMap.entrySet()) {
              // Determine whether the broker has a corresponding jmxServer
              if (Objects.equals(adr.getKey(), k)) nullJmxBroker = 0;
            }
            if (nullJmxBroker == 1) throw new RuntimeException();
          }

          existPartitionsNodeID = leaders;
          brokersWeight.setCurrentBrokerHashMap(newBrokerHashMap);
          return this.partition(topic, key, keyBytes, value, valueBytes, cluster);
        } catch (IOException e) {
          System.err.println(
              "There are brokers without Jmx Broker, you need to configure jmxServer for each broker.");
          throw new RuntimeException();
        }
      }

      brokersWeight.setCurrentBrokerHashMap(currentBrokerHashMap);
      Random rand = new Random();
      return partitionList.get(rand.nextInt(partitionList.size()));
    }

    @Override
    public void close() {
      pool.close();
    }

    @Override
    public void configure(Map<String, ?> configs) {
      try {
        var jmxAddresses =
            Objects.requireNonNull(
                (String) configs.get("jmx_servers"), "You need configure jmx_servers correctly");
        setBootstrapServers((String) configs.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

        var list = Arrays.asList((jmxAddresses).split(","));
        for (String str : list) {
          var listAddress = Arrays.asList(str.split("@"));
          addressMap.put(listAddress.get(1), listAddress.get(0));
          existPartitionsNodeID.add(listAddress.get(1));
        }
        Objects.requireNonNull(
            addressMap, "You need configure jmx_servers correctly.(JmxAddress@NodeID)");
        nodeLoadClient = new NodeLoadClient((addressMap));
      } catch (IOException e) {
        throw new RuntimeException();
      }
      pool = ThreadPool.builder().executor(nodeLoadClient).build();
    }

    private void setBootstrapServers(String bootstrapServers) {
      this.bootstrapServers = bootstrapServers;
    }
  }

  public static SmoothPartitionerFactory getFactory() {
    return FACTORY;
  }
}
