package org.astraea.partitioner.smoothPartitioner;

import static org.astraea.partitioner.nodeLoadMetric.PartitionerUtils.partitionerConfig;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.astraea.Utils;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.CostFunction;
import org.astraea.cost.ThroughputLoadCost;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.BeanCollector;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.collector.Receiver;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.partitioner.Configuration;
import org.astraea.partitioner.Dispatcher;

/**
 * Based on the jmx metrics obtained from Kafka, it records the load status of the node over a
 * period of time. Predict the future status of each node through the poisson of the load status.
 * Finally, the result of poisson is used as the weight to perform smooth weighted RoundRobin.
 */
public class SmoothWeightDispatcher implements Dispatcher {
  public static final String JMX_PORT = "jmx.port";
  private final BeanCollector beanCollector =
      BeanCollector.builder()
          .interval(Duration.ofSeconds(1))
          .numberOfObjectsPerNode(1)
          .clientCreator(MBeanClient::jndi)
          .build();
  private Optional<Integer> jmxPortDefault = Optional.empty();
  private final Map<Integer, Integer> jmxPorts = new TreeMap<>();
  private final Map<Integer, Receiver> receivers = new TreeMap<>();

  private final Collection<CostFunction> functions = List.of(new ThroughputLoadCost());

  private Map<Integer, Collection<HasBeanObject>> beans;
  private final Random random = new Random();
  // Fetch data every n seconds
  private long lastFetchTime = 0L;
  private final Lock lock = new ReentrantLock();

  @Override
  public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
    var partitions = clusterInfo.availablePartitions(topic);
    // just return first partition if there is no available partitions
    if (partitions.isEmpty()) return 0;

    // just return the only one available partition
    if (partitions.size() == 1) return partitions.iterator().next().partition();

    if (Utils.overSecond(lastFetchTime, 1) && lock.tryLock()) {
      try {
        // add new receivers for new brokers
        partitions.stream()
            .filter(p -> !receivers.containsKey(p.leader().id()))
            .forEach(
                p ->
                    receivers.put(
                        p.leader().id(), receiver(p.leader().host(), jmxPort(p.leader().id()))));

        beans =
            receivers.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().current()));

        //        functions.forEach(
        //            costFunction -> {
        //              System.out.println("1.0");
        //              System.out.println(costFunction.smoothWeightMetrics().inputCount());
        //            });

        functions.forEach(
            costFunction -> costFunction.updateLoad(ClusterInfo.of(clusterInfo, beans)));
      } finally {
        lastFetchTime = System.currentTimeMillis();
        lock.unlock();
      }
    }
    Utils.waitFor(() -> lastFetchTime != 0L);
    // Make "smooth weight choosing" on the score.
    // fetch the latest beans for each node

    var targetBroker =
        functions.stream()
            .findFirst()
            .get()
            .cost(ClusterInfo.of(clusterInfo, beans))
            .entrySet()
            .stream()
            .max(Map.Entry.comparingByValue())
            .get()
            .getKey();
    //    System.out.println(targetBroker);
    //    System.out.println("target:" + targetBroker);
    var targetPartitions =
        partitions.stream()
            .filter(p -> p.leader().id() == targetBroker)
            .collect(Collectors.toList());
    //    System.out.println(
    //        "leader:" +
    // targetPartitions.get(random.nextInt(targetPartitions.size())).leader().id());
    var test = targetPartitions.get(random.nextInt(targetPartitions.size())).partition();
    //    System.out.println(test);
    return test;
  }

  // Just add all cost function score together
  static Map<Integer, Double> costCompound(
      Map<Integer, Double> identity, Map<Integer, Double> cost) {
    cost.forEach((ID, value) -> identity.computeIfPresent(ID, (k, v) -> v + value));
    cost.forEach(identity::putIfAbsent);
    return identity;
  }

  Receiver receiver(String host, int port) {
    return beanCollector
        .register()
        .host(host)
        .port(port)
        .fetcher(
            Fetcher.of(
                functions.stream()
                    .map(CostFunction::fetcher)
                    .collect(Collectors.toUnmodifiableList())))
        .build();
  }

  @Override
  public void configure(Configuration configs) {
    var properties = partitionerConfig(configs);
    var config =
        Configuration.of(
            properties.entrySet().stream()
                .collect(
                    Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString())));

    // seeks for custom jmx ports.
    config.entrySet().stream()
        .filter(e -> e.getKey().startsWith("broker."))
        .filter(e -> e.getKey().endsWith(JMX_PORT))
        .forEach(
            e ->
                jmxPorts.put(
                    Integer.parseInt(e.getKey().split("\\.")[1]), Integer.parseInt(e.getValue())));
  }

  // visible for testing
  int jmxPort(int id) {
    if (jmxPorts.containsKey(id)) return jmxPorts.get(id);
    return jmxPortDefault.orElseThrow(
        () -> new NoSuchElementException("broker: " + id + " does not have jmx port"));
  }

  @Override
  public void close() {
    receivers.values().forEach(Utils::close);
    receivers.clear();
  }
}
