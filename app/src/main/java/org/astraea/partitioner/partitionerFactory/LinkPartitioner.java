package org.astraea.partitioner.partitionerFactory;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.astraea.partitioner.nodeLoadMetric.BrokersWeight;
import org.astraea.partitioner.nodeLoadMetric.LoadPoisson;
import org.astraea.partitioner.nodeLoadMetric.NodeLoadClient;
import org.astraea.partitioner.nodeLoadMetric.SingleThreadPool;

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

    var jmxAddresses = configs.get("jmx_servers");

    if (jmxAddresses.equals(null)) {
      throw new NullPointerException("You must configure jmx_servers correctly.");
    }

    partitioner = FACTORY.getOrCreate(ThreadSafeSmoothPartitioner.class, configs);
  }

  public Partitioner getPartitioner() {
    return partitioner;
  }

  public static class ThreadSafeSmoothPartitioner implements Partitioner {
    private NodeLoadClient nodeLoadClient;
    private Map<String, ?> smoothConfigs;
    private SingleThreadPool pool;

    ThreadSafeSmoothPartitioner() throws MalformedURLException {
      nodeLoadClient = new NodeLoadClient((Map<String, String>) smoothConfigs.get("jmx_servers"));
      pool = SingleThreadPool.builder().build(nodeLoadClient);
    }

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
      brokersWeight.setBrokerHashMap();
      Map.Entry<String, int[]> maxWeightServer = null;

      int allWeight = brokersWeight.getAllWeight();
      HashMap<String, int[]> currentBrokerHashMap = brokersWeight.getBrokerHashMap();

      for (Map.Entry<String, int[]> item : currentBrokerHashMap.entrySet()) {
        Map.Entry<String, int[]> currentServer = item;
        if (maxWeightServer == null
            || currentServer.getValue()[1] > maxWeightServer.getValue()[1]) {
          maxWeightServer = currentServer;
        }
      }
      assert maxWeightServer != null;
      currentBrokerHashMap.put(
          maxWeightServer.getKey(),
          new int[] {maxWeightServer.getValue()[0], maxWeightServer.getValue()[1] - allWeight});
      brokersWeight.setCurrentBrokerHashMap(currentBrokerHashMap);

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
      smoothConfigs = configs;
    }
  }

  public static SmoothPartitionerFactory getFactory() {
    return FACTORY;
  }
}
