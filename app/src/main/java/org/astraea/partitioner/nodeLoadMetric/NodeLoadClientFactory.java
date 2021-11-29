package org.astraea.partitioner.nodeLoadMetric;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class NodeLoadClientFactory {
  private final Object lock = new Object();
  private final Map<Map<String, ?>, Integer> count;
  private final Map<Map<String, ?>, NodeLoadClient> instances;
  private final Map<Map<String, ?>, NodeLoadClient> nodeLoadClientMap;
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
      var jmxAddress = Arrays.asList((jmxAddresses).split(","));
      Objects.requireNonNull(
          jmxAddress, "You must configure jmx_servers correctly.(JmxAddress@NodeID)");
      var nodeLoadClient = new NodeLoadClient(jmxAddress);
      var proxy =
          new NodeLoadClient(jmxAddress) {

            @Override
            public LoadPoisson getLoadPoisson() {
              return nodeLoadClient.getLoadPoisson();
            }

            @Override
            public void close() {
              synchronized (lock) {
                var current = count.get(configs);
                if (current == 1) {
                  try {
                    nodeLoadClient.close();
                  } catch (Exception e) {
                    e.printStackTrace();
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
      return proxy;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Map<Map<String, ?>, NodeLoadClient> getnodeLoadClientMap() {
    return this.nodeLoadClientMap;
  }
}
