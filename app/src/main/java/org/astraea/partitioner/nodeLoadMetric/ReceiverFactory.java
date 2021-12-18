package org.astraea.partitioner.nodeLoadMetric;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.astraea.metrics.collector.BeanCollector;
import org.astraea.metrics.collector.Receiver;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.metrics.kafka.KafkaMetrics;

/** create a single BeanCollector to avoid waste of resources */
public class ReceiverFactory {
  private final Object lock = new Object();
  private final List<Receiver> receiversList = new ArrayList<>();
  private final BeanCollector beanCollector;
  private final Map<String, Integer> count = new HashMap<>();

  /** create a factory with specific comparator. * */
  public ReceiverFactory() {
    beanCollector =
        BeanCollector.builder()
            .interval(Duration.ofSeconds(1))
            .numberOfObjectsPerNode(10)
            .clientCreator(MBeanClient::jndi)
            .build();
  }

  /**
   * @return create new receivers,if the receiver of the node does not exist.Then return
   *     receiversList for all node's Receivers.Otherwise, it returns the existent receiversList.
   */
  public List<Receiver> receiversList(Map<String, Integer> jmxAddresses) {
    synchronized (lock) {
      jmxAddresses
          .entrySet()
          .forEach(
              entry -> {
                if (!count.containsKey(nodeKey(entry))) {
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
                  count.put(nodeKey(entry), 1);
                } else {
                  count.put(nodeKey(entry), count.get(nodeKey(entry)) + 1);
                }
              });
    }
    return receiversList;
  }

  public void close(Map<String, Integer> jmxAddresses) {
    synchronized (lock) {
      jmxAddresses
          .entrySet()
          .forEach(
              entry -> {
                if (count.containsKey(nodeKey(entry))) {
                  if (count.get(nodeKey(entry)) == 1) {
                    receiversList.stream()
                        .filter(
                            receiver ->
                                receiver.host().equals(entry.getKey())
                                    && receiver.port() == entry.getValue())
                        .forEach(Receiver::close);
                    count.remove(nodeKey(entry));
                  } else {
                    count.put(nodeKey(entry), count.get(nodeKey(entry)) - 1);
                  }
                }
              });
    }
  }

  private String nodeKey(Map.Entry<String, Integer> entry) {
    return entry.getKey() + ":" + entry.getValue();
  }

  // visible for testing
  public int factoryCount(Map<String, Integer> jmxAddresses) {
    var testCount = new AtomicInteger(-1);
    jmxAddresses.entrySet().forEach(entry -> testCount.set(count.getOrDefault(nodeKey(entry), 0)));
    return testCount.get();
  }
}
