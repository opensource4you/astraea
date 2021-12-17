package org.astraea.partitioner.nodeLoadMetric;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.astraea.metrics.collector.BeanCollector;
import org.astraea.metrics.collector.Receiver;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.metrics.kafka.KafkaMetrics;

/** create a single BeanCollector to avoid waste of resources */
public class receiverFactory {
  private final Object lock = new Object();
  private final List<Receiver> receiversList = new ArrayList<>();
  private final BeanCollector beanCollector;
  private final Map<Map.Entry<String, Integer>, Integer> count = new HashMap<>();

  /** create a factory with specific comparator. * */
  public receiverFactory() {
    BiFunction<String, Integer, MBeanClient> clientCreator = MBeanClient::jndi;
    beanCollector =
        BeanCollector.builder()
            .interval(Duration.ofSeconds(1))
            .numberOfObjectsPerNode(10)
            .clientCreator(clientCreator)
            .build();
  }

  /**
   * @return create a new BeanCollector if there is no exist BeanCollector.Then return Receiver for
   *     BeanCollector.Otherwise, it returns the existent Receiver.
   */
  public List<Receiver> receiversList(Map<String, Integer> jmxAddresses) {
    synchronized (lock) {
      for (Map.Entry<String, Integer> entry : jmxAddresses.entrySet()) {
        if (!count.containsKey(entry)) {
          receiversList.add(
              beanCollector
                  .register()
                  .host(entry.getKey())
                  .port(entry.getValue())
                  .metricsGetter(KafkaMetrics.BrokerTopic.BytesInPerSec::fetch)
                  .build());
          receiversList.add(
              beanCollector
                  .register()
                  .host(entry.getKey())
                  .port(entry.getValue())
                  .metricsGetter(KafkaMetrics.BrokerTopic.BytesOutPerSec::fetch)
                  .build());
          count.put(entry, 1);
        } else {
          count.put(entry, count.get(entry) + 1);
        }
      }
    }
    return receiversList;
  }

  public void close(Map<String, Integer> jmxAddresses) {
    synchronized (lock) {
      for (Map.Entry<String, Integer> entry : jmxAddresses.entrySet()) {
        if (count.get(entry) == 1) {
          receiversList.stream()
              .filter(
                  receiver ->
                      receiver.host().equals(entry.getKey()) && receiver.port() == entry.getValue())
              .forEach(Receiver::close);
          count.remove(entry);
        } else {
          count.put(entry, count.get(entry) - 1);
        }
      }
    }
  }

  // visible for testing
  public int factoryCount(Map<String, Integer> jmxAddresses) {
    var testCount = -1;
    for (Map.Entry<String, Integer> entry : jmxAddresses.entrySet()) {
      if (count.containsKey(entry)) {
        testCount = count.get(entry);
      } else {
        testCount = 0;
      }
    }
    return testCount;
  }
}
