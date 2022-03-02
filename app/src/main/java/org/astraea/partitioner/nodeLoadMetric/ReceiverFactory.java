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
  private Map<String, Integer> jmxAddresses;

  /** create a factory with specific comparator. * */
  public ReceiverFactory() {
    beanCollector =
        BeanCollector.builder()
            .interval(Duration.ofSeconds(1))
            .numberOfObjectsPerNode(1)
            .clientCreator(MBeanClient::jndi)
            .build();
  }

  /**
   * @return create new receivers,if the receiver of the node does not exist.Then return
   *     receiversList for all node's Receivers.Otherwise, it returns the existent receiversList.
   */
  public List<Receiver> receiversList(Map<String, Integer> jmxAddresses) {
    this.jmxAddresses = jmxAddresses;
    synchronized (lock) {
      jmxAddresses.forEach(
          (host, port) -> {
            if (!count.containsKey(nodeKey(host, port))) {
              receiversList.add(
                  beanCollector
                      .register()
                      .host(host)
                      .port(port)
                      .fetcher(
                          client -> List.of(KafkaMetrics.BrokerTopic.BytesInPerSec.fetch(client)))
                      .build());
              receiversList.add(
                  beanCollector
                      .register()
                      .host(host)
                      .port(port)
                      .fetcher(
                          client -> List.of(KafkaMetrics.BrokerTopic.BytesOutPerSec.fetch(client)))
                      .build());
              receiversList.add(
                  beanCollector
                      .register()
                      .host(host)
                      .port(port)
                      .fetcher(client -> List.of(KafkaMetrics.Host.jvmMemory(client)))
                      .build());
              count.put(nodeKey(host, port), 1);
            } else {
              count.put(nodeKey(host, port), count.get(nodeKey(host, port)) + 1);
            }
          });
    }
    return receiversList;
  }

  public void close() {
    synchronized (lock) {
      jmxAddresses.forEach(
          (host, port) -> {
            if (count.containsKey(nodeKey(host, port))) {
              if (count.get(nodeKey(host, port)) == 1) {
                receiversList.stream()
                    .filter(receiver -> receiver.host().equals(host) && receiver.port() == port)
                    .forEach(Receiver::close);
                count.remove(nodeKey(host, port));
              } else {
                count.put(nodeKey(host, port), count.get(nodeKey(host, port)) - 1);
              }
            }
          });
    }
  }

  private String nodeKey(String host, Integer port) {
    return host + ":" + port;
  }

  // visible for testing
  int factoryCount(Map<String, Integer> jmxAddresses) {
    var testCount = new AtomicInteger(-1);
    jmxAddresses.forEach((host, port) -> testCount.set(count.getOrDefault(nodeKey(host, port), 0)));
    return testCount.get();
  }
}
