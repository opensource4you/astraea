package org.astraea.partitioner.nodeLoadMetric;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.astraea.metrics.BeanCollector;
import org.astraea.metrics.HasBeanObject;

public class BeanCollectorFactory {
  private final Object lock = new Object();
  private final Map<Map<String, ?>, Integer> count;
  private final Map<Map<String, ?>, BeanCollector> instances;

  /**
   * create a factory with specific comparator. *
   *
   * @param comparator used to compare the partitioners. There is no new producer if the comparator
   *     returns 0 (equal).
   */
  public BeanCollectorFactory(Comparator<Map<String, ?>> comparator) {
    this.count = new TreeMap<>(comparator);
    this.instances = new TreeMap<>(comparator);
  }

  /**
   * @param configs as map key
   * @return create a new BeanCollector if there is no matched BeanCollector (checked by
   *     comparator). Otherwise, it returns the existent BeanCollector.
   */
  public BeanCollector getOrCreate(Map<String, ?> configs) {
    synchronized (lock) {
      var beanCollector = instances.get(configs);

      if (beanCollector != null) {
        count.put(configs, count.get(configs) + 1);
        return beanCollector;
      }
      return create(configs);
    }
  }

  private BeanCollector create(Map<String, ?> configs) {
    var beanCollector =
        BeanCollector.builder()
            .interval(Duration.ofMillis(1000))
            .numberOfObjectsPerNode(10)
            .build();
    var proxy =
        new BeanCollector(beanCollector.pool(), beanCollector.numberOfObjectsPerNode()) {

          public Map<Node, List<HasBeanObject>> objects() {
            return beanCollector.objects();
          }

          public int numberOfObjects() {
            return beanCollector.numberOfObjects();
          }

          public Register register() {
            return beanCollector.register();
          }

          public List<Node> nodes() {
            return beanCollector.nodes();
          }

          public void close() {
            beanCollector.close();
          }
        };

    count.put(configs, 1);
    instances.put(configs, proxy);
    return proxy;
  }

  public void close(Map<String, ?> configs) {
    synchronized (lock) {
      var current = count.get(configs);
      if (current == 1) {
        count.remove(configs);
        instances.remove(configs);
      } else count.put(configs, current - 1);
    }
  }

  public Map<Map<String, ?>, Integer> factoryCount() {
    return this.count;
  }

  public Map<Map<String, ?>, BeanCollector> getInstances() {
    return this.instances;
  }
}
