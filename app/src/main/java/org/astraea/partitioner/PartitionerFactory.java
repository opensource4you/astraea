package org.astraea.partitioner;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class PartitionerFactory {
  private final Object lock = new Object();
  private final Map<Map<String, ?>, Integer> count;
  private final Map<Map<String, ?>, Partitioner> instances;

  PartitionerFactory() {
    this((o1, o2) -> o1.equals(o2) ? 0 : Integer.compare(o1.hashCode(), o2.hashCode()));
  }

  /**
   * create a factory with specific comparator.
   *
   * @param comparator used to compare the partitioners. There is no new producer if the comparator
   *     returns 0 (equal).
   */
  PartitionerFactory(Comparator<Map<String, ?>> comparator) {
    this.count = new TreeMap<>(comparator);
    this.instances = new TreeMap<>(comparator);
  }

  Partitioner getOrCreate(Class<? extends Partitioner> clz, Map<String, ?> configs) {
    synchronized (lock) {
      var partitioner = instances.get(configs);
      if (partitioner != null) {
        count.put(configs, count.get(configs) + 1);
        return partitioner;
      }
      return create(clz, configs);
    }
  }

  private Partitioner create(Class<? extends Partitioner> clz, Map<String, ?> configs) {
    try {
      var partitioner = clz.getDeclaredConstructor().newInstance();
      partitioner.configure(configs);
      var proxy =
          new Partitioner() {
            @Override
            public int partition(
                String topic,
                Object key,
                byte[] keyBytes,
                Object value,
                byte[] valueBytes,
                Cluster cluster) {
              return partitioner.partition(topic, key, keyBytes, value, valueBytes, cluster);
            }

            @Override
            public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
              partitioner.onNewBatch(topic, cluster, prevPartition);
            }

            @Override
            public void close() {
              synchronized (lock) {
                var current = count.get(configs);
                if (current == 1) {
                  try {
                    partitioner.close();
                  } finally {
                    count.remove(configs);
                    instances.remove(configs);
                  }
                } else count.put(configs, current - 1);
              }
            }

            @Override
            public void configure(Map<String, ?> configs) {
              partitioner.configure(configs);
            }
          };
      count.put(configs, 1);
      instances.put(configs, proxy);
      return proxy;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
