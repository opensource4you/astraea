package org.astraea.partitioner.nodeLoadMetric;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.astraea.concurrent.ThreadPool;
import org.astraea.metrics.collector.BeanCollector;
import org.astraea.metrics.collector.Receiver;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.metrics.kafka.KafkaMetrics;

/** create a single BeanCollector to avoid waste of resources */
public class BeanCollectorFactory {
  private final Object lock = new Object();
  private int count = 0;
  private ThreadPool threadPool;
  private final List<Receiver> receiversList = new ArrayList<>();

  /** create a factory with specific comparator. * */
  public BeanCollectorFactory() {}

  /**
   * @return create a new BeanCollector if there is no exist BeanCollector.Then return Receiver for
   *     BeanCollector.Otherwise, it returns the existent Receiver.
   */
  public List<Receiver> receiversList(Map<String, Integer> jmxAddresses) {
    if (count == 0) {
      synchronized (lock) {
        for (Map.Entry<String, Integer> entry : jmxAddresses.entrySet()) {
          var mbeanClient = MBeanClient.jndi(entry.getKey(), entry.getValue());
          BiFunction<String, Integer, MBeanClient> clientCreator = (host, port) -> mbeanClient;
          var beanCollector =
              BeanCollector.builder()
                  .interval(Duration.ofSeconds(1))
                  .numberOfObjectsPerNode(10)
                  .clientCreator(clientCreator)
                  .build();
          receiversList.add(
              beanCollector
                  .register()
                  .host(entry.getKey())
                  .port(entry.getValue())
                  .metricsGetter(KafkaMetrics.BrokerTopic.BytesOutPerSec::fetch)
                  .build());
          receiversList.add(
              beanCollector
                  .register()
                  .host(entry.getKey())
                  .port(entry.getValue())
                  .metricsGetter(KafkaMetrics.BrokerTopic.BytesInPerSec::fetch)
                  .build());
        }
        threadPool =
            ThreadPool.builder()
                .runnables(
                    receiversList.stream()
                        .map(receiver -> ((Runnable) receiver::current))
                        .collect(Collectors.toList()))
                .build();
        count++;
      }
    } else {
      count++;
    }
    return receiversList;
  }

  public void close() {
    synchronized (lock) {
      if (count == 1) {
        threadPool.close();
        count = 0;
      } else count--;
    }
  }

  public int factoryCount() {
    return this.count;
  }
}
