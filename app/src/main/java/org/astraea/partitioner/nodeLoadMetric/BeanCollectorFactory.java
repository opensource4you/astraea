package org.astraea.partitioner.nodeLoadMetric;

import java.time.Duration;
import org.astraea.metrics.BeanCollector;

/** create a single BeanCollector to avoid waste of resources */
public class BeanCollectorFactory {
  private final Object lock = new Object();
  private int count = 0;
  private BeanCollector beanCollector;

  /** create a factory with specific comparator. * */
  public BeanCollectorFactory() {}

  /**
   * @return create a new BeanCollector if there is no matched BeanCollector (checked by
   *     comparator). Otherwise, it returns the existent BeanCollector.
   */
  public BeanCollector beanCollector() {
    synchronized (lock) {
      if (count == 0) {
        beanCollector =
            BeanCollector.builder()
                .interval(Duration.ofMillis(1000))
                .numberOfObjectsPerNode(10)
                .build();
        count++;
      } else {
        count++;
      }
      return beanCollector;
    }
  }

  public void close() {
    synchronized (lock) {
      if (count == 1) {
        beanCollector.close();
        count = 0;
      } else count--;
    }
  }

  public int factoryCount() {
    return this.count;
  }
}
