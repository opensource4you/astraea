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

public class LinkPartitioner implements Partitioner {
  private static HashMap<String, String> jmxAddresses;

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
  public void configure(Map<String, ?> configs) {
    jmxAddresses = (HashMap<String, String>) configs.get("jmx_server");
    partitioner = FACTORY.getOrCreate(ThreadSafeSmoothPartitioner.class, configs);
  }

  public Partitioner getPartitioner() {
    return partitioner;
  }

  public static class ThreadSafeSmoothPartitioner implements Partitioner {
    private NodeLoadClient nodeLoadClient;
    private Thread loadThread;

    ThreadSafeSmoothPartitioner() throws MalformedURLException {
      nodeLoadClient = new NodeLoadClient(LinkPartitioner.jmxAddresses);

      loadThread = new Thread(nodeLoadClient);
      loadThread.start();
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
      nodeLoadClient.shouldDownNow();
    }

    @Override
    public void configure(Map<String, ?> configs) {}
  }

  public static SmoothPartitionerFactory getFactory() {
    return FACTORY;
  }
}
