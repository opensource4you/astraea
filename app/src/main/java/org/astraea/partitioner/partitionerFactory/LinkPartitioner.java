package org.astraea.partitioner.partitionerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.astraea.concurrent.ThreadPool;
import org.astraea.partitioner.nodeLoadMetric.NodeLoadClient;

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
    /**
     * Record the current weight of each node according to Poisson calculation and the weight after
     * partitioner calculation.
     */
    private HashMap<String, int[]> allBrokersWeight = new HashMap<>();

    private NodeLoadClient nodeLoadClient;
    private ThreadPool pool;

    ThreadSafeSmoothPartitioner() {}

    @Override
    public int partition(
        String topic,
        Object key,
        byte[] keyBytes,
        Object value,
        byte[] valueBytes,
        Cluster cluster) {
      Map.Entry<String, int[]> maxWeightServer = null;
      updateBrokersWeight();
      var allWeight = getAllWeight();
      HashMap<String, int[]> currentBrokers = getAllBrokersWeight();

      for (Map.Entry<String, int[]> item : currentBrokers.entrySet()) {
        if (maxWeightServer == null || item.getValue()[1] > maxWeightServer.getValue()[1]) {
          maxWeightServer = item;
        }
        currentBrokers.put(
            item.getKey(), new int[] {item.getValue()[0], item.getValue()[1] + item.getValue()[0]});
      }
      assert maxWeightServer != null;
      currentBrokers.put(
          maxWeightServer.getKey(),
          new int[] {maxWeightServer.getValue()[0], maxWeightServer.getValue()[1] - allWeight});
      setCurrentBrokers(currentBrokers);

      ArrayList<Integer> partitionList = new ArrayList<>();
      for (PartitionInfo partitionInfo :
          cluster.partitionsForNode(Integer.parseInt(maxWeightServer.getKey()))) {
        partitionList.add(partitionInfo.partition());
      }
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
                (String) configs.get("jmx_servers"), "You must configure jmx_servers correctly");
        var list = Arrays.asList((jmxAddresses).split(","));
        Map<String, String> jmxAddress = new HashMap<>();
        for (String str : list) {
          var listAddress = Arrays.asList(str.split("@"));
          jmxAddress.put(listAddress.get(1), listAddress.get(0));
        }
        Objects.requireNonNull(
            jmxAddress, "You must configure jmx_servers correctly.(JmxAddress@NodeID)");
        nodeLoadClient = new NodeLoadClient((jmxAddress));
      } catch (IOException e) {
        throw new RuntimeException();
      }
      pool = ThreadPool.builder().executor(nodeLoadClient).build();
    }
    /** Change the weight of the node according to the current Poisson. */
    private synchronized void updateBrokersWeight() {
      Map<String, Double> allPoisson = nodeLoadClient.getLoadPoisson().getAllPoisson();

      for (Map.Entry<String, Double> entry : allPoisson.entrySet()) {
        if (!allBrokersWeight.containsKey(entry.getKey())) {
          allBrokersWeight.put(entry.getKey(), new int[] {(int) ((1 - entry.getValue()) * 20), 0});
        } else {
          allBrokersWeight.put(
              entry.getKey(),
              new int[] {
                (int) ((1 - entry.getValue()) * 20), allBrokersWeight.get(entry.getKey())[1]
              });
        }
      }
    }

    private synchronized int getAllWeight() {
      return allBrokersWeight.values().stream().mapToInt(vs -> vs[0]).sum();
    }

    private synchronized HashMap<String, int[]> getAllBrokersWeight() {
      return allBrokersWeight;
    }

    private synchronized void setCurrentBrokers(HashMap<String, int[]> currentBrokers) {
      allBrokersWeight = currentBrokers;
    }
  }

  public static SmoothPartitionerFactory getFactory() {
    return FACTORY;
  }
}
