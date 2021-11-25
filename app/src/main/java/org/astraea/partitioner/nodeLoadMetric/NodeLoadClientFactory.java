package org.astraea.partitioner.nodeLoadMetric;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.astraea.concurrent.ThreadPool;

public class NodeLoadClientFactory {
  private final Object lock = new Object();
  private final Map<Map<String, ?>, Integer> count;
  private final Map<Map<String, ?>, NodeLoadClient> instances;
  private final Map<Map<String, ?>, NodeLoadClient> nodeLoadClientMap;
  private ThreadPool pool;
  /**
   * create a factory with specific comparator.
   *
   * @param comparator used to compare the partitioners. There is no new producer if the comparator
   *     returns 0 (equal).
   */
  public NodeLoadClientFactory(Comparator<Map<String, ?>> comparator) {
    this.count = new TreeMap<>(comparator);
    this.instances = new TreeMap<>(comparator);
    this.nodeLoadClientMap = new TreeMap<>(comparator);
  }

  /**
   * @param clz nodeLoadClient class
   * @param configs used to initialize new nodeLoadClient
   * @return create a new nodeLoadClient if there is no matched nodeLoadClient (checked by
   *     comparator). Otherwise, it returns the existent nodeLoadClient.
   */
  public NodeLoadClient getOrCreate(Class<NodeLoadClient> clz, Map<String, ?> configs) {
    synchronized (lock) {
      var nodeLoadClient = instances.get(configs);
      if (nodeLoadClient != null) {
        count.put(configs, count.get(configs) + 1);
        return nodeLoadClient;
      }
      return create(clz, configs);
    }
  }

  private NodeLoadClient create(Class<NodeLoadClient> clz, Map<String, ?> configs) {
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
      var nodeLoadClient = new NodeLoadClient(jmxAddress);
      var proxy =
          new NodeLoadClient(jmxAddress) {
            @Override
            public void refreshNodesMetrics() {
              nodeLoadClient.refreshNodesMetrics();
            }

            @Override
            public synchronized int avgLoadCount() {
              return nodeLoadClient.avgLoadCount();
            }

            @Override
            public LoadPoisson getLoadPoisson() {
              return nodeLoadClient.getLoadPoisson();
            }

            @Override
            public Map<String, Integer> nodeOverLoadCount() {
              return nodeLoadClient.nodeOverLoadCount();
            }

            @Override
            public State execute() throws InterruptedException {
              nodeLoadClient.execute();
              return null;
            }

            @Override
            public void close() {
              synchronized (lock) {
                var current = count.get(configs);
                if (current == 1) {
                  try {
                    nodeLoadClient.close();
                  } finally {
                    count.remove(configs);
                    instances.remove(configs);
                    nodeLoadClientMap.remove(configs);
                  }
                } else count.put(configs, current - 1);
              }
            }
          };
      count.put(configs, 1);
      instances.put(configs, proxy);
      nodeLoadClientMap.put(configs, nodeLoadClient);
      pool = ThreadPool.builder().executor(proxy).build();
      Runtime.getRuntime().addShutdownHook(new Thread(() ->pool.close()));
      return proxy;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Map<Map<String, ?>, NodeLoadClient> getnodeLoadClientMap() {
    return this.nodeLoadClientMap;
  }

    public ThreadPool getPool() {
        return pool;
    }
}
