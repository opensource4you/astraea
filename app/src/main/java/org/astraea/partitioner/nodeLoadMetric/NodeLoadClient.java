package org.astraea.partitioner.nodeLoadMetric;

import static java.lang.Double.sum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.astraea.Utils;
import org.astraea.metrics.BeanCollector;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.kafka.KafkaMetrics;

/**
 * this clas is responsible for obtaining jmx metrics from BeanCollector and calculating the
 * overload status of each node through them.
 */
public class NodeLoadClient {

  private BeanCollector beanCollector;
  private Map<String, ?> configs;
  private Map<Map.Entry<String, Integer>, Map<String, List<HasBeanObject>>> valueMetrics;

  private static final BeanCollectorFactory FACTORY =
      new BeanCollectorFactory(
          Comparator.comparing(o -> o.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).toString()));

  public NodeLoadClient(Map<String, String> jmxAddresses, Map<String, ?> configs)
      throws IOException {
    for (HashMap.Entry<String, String> entry : jmxAddresses.entrySet()) {
      this.beanCollector = FACTORY.getOrCreate(configs);
      beanCollector
          .register()
          .host(entry.getKey())
          .port(Integer.parseInt(entry.getValue()))
          .metricsGetter(KafkaMetrics.BrokerTopic.BytesOutPerSec::fetch)
          .build();
      beanCollector
          .register()
          .host(entry.getKey())
          .port(Integer.parseInt(entry.getValue()))
          .metricsGetter(KafkaMetrics.BrokerTopic.BytesInPerSec::fetch)
          .build();
      this.configs = configs;
      Utils.waitFor(() -> beanCollector.numberOfObjects() > 0);
      System.out.println(beanCollector.nodes().size());
      System.out.println(beanCollector.numberOfObjects());
    }
  }

  /**
   * @param cluster the cluster from the partitioner
   * @return each node overLoad count in preset time
   */
  public Map<Integer, Integer> nodesOverLoad(Cluster cluster) {
    var valuableObjects = new HashMap<Map.Entry<String, Integer>, List<HasBeanObject>>();
    var addresses = cluster.nodes().stream().map(Node::host).collect(Collectors.toList());

    System.out.println(beanCollector.nodes().size());
    var nodesMetrics = beanCollector.objects();
    System.out.println(nodesMetrics);

    for (String host : addresses) {
      var valueNode =
          nodesMetrics.entrySet().stream()
              .filter(entry -> Objects.equals(entry.getKey().host(), host))
              .findAny()
              .orElse(null);
      var addressEntrySet = new HashMap<String, Integer>();
      addressEntrySet.put(valueNode.getKey().host(), valueNode.getKey().port());
      valuableObjects.put(
          addressEntrySet.entrySet().stream().findAny().get(), valueNode.getValue());
    }
    valueMetrics = metricsNameObjects(valuableObjects);
    var eachBrokerMsgPerSec = brokersMsgPerSec();

    var overLoadCount = new HashMap<Map.Entry<String, Integer>, Integer>();
    eachBrokerMsgPerSec.keySet().forEach(e -> overLoadCount.put(e, 0));
    var i = 0;
    while (i < 10) {
      var avg = avgMsgSec(i);
      var eachMsg = brokersMsgSec(i);
      var standardDeviation = standardDeviationImperative(eachMsg, avg);
      for (Map.Entry<Map.Entry<String, Integer>, Double> entry : eachMsg.entrySet()) {
        if (entry.getValue() > (avg + standardDeviation)) {
          overLoadCount.put(entry.getKey(), overLoadCount.get(entry.getKey()) + 1);
        }
      }
      i++;
    }
    var nodeID = new HashMap<Integer, Integer>();
    for (Map.Entry<Map.Entry<String, Integer>, Integer> entry : overLoadCount.entrySet()) {
      cluster.nodes().stream()
          .filter(node -> node.host().equals(entry.getKey().getKey()))
          .map(node -> nodeID.put(node.id(), entry.getValue()));
    }
    return nodeID;
  }

  /** @return data transfer per second per node */
  private Map<Map.Entry<String, Integer>, List<Double>> brokersMsgPerSec() {
    var eachMsg = new HashMap<Map.Entry<String, Integer>, List<Double>>();

    for (Map.Entry<Map.Entry<String, Integer>, Map<String, List<HasBeanObject>>> entry :
        valueMetrics.entrySet()) {
      List<Double> sumList = new ArrayList<>();
      var outMsg = entry.getValue().get(KafkaMetrics.BrokerTopic.BytesOutPerSec.toString());
      var inMsg = entry.getValue().get(KafkaMetrics.BrokerTopic.BytesInPerSec.toString());

      IntStream.range(
              0, valueMetrics.values().stream().map(s -> s.values()).findFirst().get().size())
          .mapToObj(
              i ->
                  sumList.add(
                      sum(
                          Double.parseDouble(
                              outMsg
                                  .get(i)
                                  .beanObject()
                                  .getAttributes()
                                  .get("MeanRate")
                                  .toString()),
                          Double.parseDouble(
                              inMsg
                                  .get(i)
                                  .beanObject()
                                  .getAttributes()
                                  .get("MeanRate")
                                  .toString()))));

      eachMsg.put(entry.getKey(), sumList);
    }
    return eachMsg;
  }

  public List<Double> avgBrokersMsgPerSec(Map<Map.Entry<String, Integer>, List<Double>> eachMsg) {
    return IntStream.range(0, eachMsg.values().stream().findFirst().get().size())
        .mapToDouble(i -> eachMsg.values().stream().map(s -> s.get(i)).reduce(0.0, Double::sum))
        .map(sum -> sum / eachMsg.size())
        .boxed()
        .collect(Collectors.toList());
  }

  public double standardDeviationImperative(
      Map<Map.Entry<String, Integer>, Double> eachMsg, double avgBrokersMsgPerSec) {
    var variance = 0.0;
    for (Map.Entry<Map.Entry<String, Integer>, Double> entry : eachMsg.entrySet()) {
      variance +=
          (entry.getValue() - avgBrokersMsgPerSec) * (entry.getValue() - avgBrokersMsgPerSec);
    }
    return Math.sqrt(variance / eachMsg.size());
  }

  /**
   * @param valueMetrics node metrics that exists in topic
   * @return the name and corresponding data of the metrics obtained by each node
   */
  public Map<Map.Entry<String, Integer>, Map<String, List<HasBeanObject>>> metricsNameObjects(
      HashMap<Map.Entry<String, Integer>, List<HasBeanObject>> valueMetrics) {
    Map<Map.Entry<String, Integer>, Map<String, List<HasBeanObject>>> result = new HashMap<>();
    for (Map.Entry<Map.Entry<String, Integer>, List<HasBeanObject>> entry :
        valueMetrics.entrySet()) {
      var metricsName =
          entry.getValue().stream()
              .filter(distinctByKey(b -> b.beanObject().getProperties().values()))
              .map(b -> b.beanObject().getProperties().values().stream().findAny().get())
              .collect(Collectors.toList());
      var objectName =
          metricsName.stream()
              .collect(
                  Collectors.toMap(
                      Function.identity(),
                      name ->
                          entry.getValue().stream()
                              .filter(
                                  mbean ->
                                      mbean.beanObject().getProperties().values().stream()
                                          .findAny()
                                          .get()
                                          .equals(name))
                              .collect(Collectors.toList())));
      result.put(entry.getKey(), objectName);
    }
    return result;
  }

  private Map<Map.Entry<String, Integer>, Double> brokersMsgSec(int index) {
    return brokersMsgPerSec().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get(index)));
  }

  private double avgMsgSec(int index) {
    return avgBrokersMsgPerSec(brokersMsgPerSec()).get(index);
  }

  public void close() {
    FACTORY.close(configs);
  }

  static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
    Map<Object, Boolean> seen = new ConcurrentHashMap<>();
    return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
  }

  public BeanCollector beanCollector() {
    return beanCollector;
  }

  public BeanCollectorFactory getFactory() {
    return FACTORY;
  }
}
