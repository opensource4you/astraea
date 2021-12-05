package org.astraea.partitioner.nodeLoadMetric;

import static java.lang.Double.sum;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
  private Map<Integer, Map.Entry<Map.Entry<String, Integer>, Map<String, List<HasBeanObject>>>>
      valueMetrics;

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
    }
  }

  /**
   * @param cluster the cluster from the partitioner
   * @return each node overLoad count in preset time
   */
  public Map<Integer, Integer> nodesOverLoad(Cluster cluster) throws UnknownHostException {
    var valuableObjects = new HashMap<Map.Entry<String, Integer>, List<HasBeanObject>>();
    var valuableNodeID =
        new HashMap<Integer, Map.Entry<Map.Entry<String, Integer>, List<HasBeanObject>>>();
    var addresses =
        cluster.nodes().stream()
            .collect(
                Collectors.toMap(node -> node.id(), node -> Map.entry(node.host(), node.port())));

    var nodesMetrics = beanCollector.objects();

    for (Map.Entry<Integer, Map.Entry<String, Integer>> hostPort : addresses.entrySet()) {
      Map.Entry<BeanCollector.Node, List<HasBeanObject>> valueNode;
      String regex =
          "((25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)\\.){3}"
              + "(25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)$";
      if (!hostPort.getValue().getKey().matches(regex)) {
        var localhost =
            InetAddress.getByName(hostPort.getValue().getKey()).toString().split("/")[1];
        valueNode =
            nodesMetrics.entrySet().stream()
                .filter(
                    entry ->
                        ((valuableObjects.keySet().stream()
                                        .map(n -> Objects.equals(n.getKey(), localhost))
                                        .anyMatch(n -> n.equals(true))
                                    && valuableObjects.keySet().stream()
                                        .map(
                                            n ->
                                                !Objects.equals(
                                                    n.getValue(), hostPort.getValue().getValue()))
                                        .anyMatch(n -> n.equals(true)))
                                || Objects.equals(valuableObjects.size(), 0))
                            && Objects.equals(entry.getKey().host(), localhost))
                .findAny()
                .orElse(null);
      } else {
        valueNode =
            nodesMetrics.entrySet().stream()
                .filter(
                    entry ->
                        ((valuableObjects.keySet().stream()
                                        .map(
                                            n ->
                                                Objects.equals(
                                                    n.getKey(), hostPort.getValue().getKey()))
                                        .anyMatch(n -> n.equals(true))
                                    && valuableObjects.keySet().stream()
                                        .map(
                                            n ->
                                                Objects.equals(
                                                    n.getValue(), hostPort.getValue().getValue()))
                                        .anyMatch(n -> n.equals(true)))
                                || Objects.equals(valuableObjects.size(), 0))
                            && Objects.equals(entry.getKey().host(), hostPort.getValue().getKey()))
                .findAny()
                .orElse(null);
      }
      var addressEntrySet = new HashMap<String, Integer>();

      if (valueNode != null) {
        addressEntrySet.put(valueNode.getKey().host(), valueNode.getKey().port());
        valuableObjects.put(
            addressEntrySet.entrySet().stream().findAny().get(), valueNode.getValue());
        valuableNodeID.put(
            hostPort.getKey(),
            Map.entry(addressEntrySet.entrySet().stream().findAny().get(), valueNode.getValue()));
      }
    }

    valueMetrics = metricsNameObjects(valuableNodeID);
    var eachBrokerMsgPerSec = brokersMsgPerSec();

    var overLoadCount = new HashMap<Integer, Integer>();
    eachBrokerMsgPerSec.keySet().forEach(e -> overLoadCount.put(e, 0));
    var i = 0;
    var minRecordSec =
        brokersMsgPerSec().values().stream()
            .map(n -> n.size())
            .min(Comparator.comparing(Integer::intValue))
            .get();
    while (i < minRecordSec) {
      var avg = avgMsgSec(i);
      var eachMsg = brokersMsgSec(i);
      var standardDeviation = standardDeviationImperative(eachMsg, avg);
      for (Map.Entry<Integer, Double> entry : eachMsg.entrySet()) {
        if (entry.getValue() > (avg + standardDeviation)) {
          overLoadCount.put(entry.getKey(), overLoadCount.get(entry.getKey()) + 1);
        }
      }
      i++;
    }
    return overLoadCount;
  }

  /** @return data transfer per second per node */
  private Map<Integer, List<Double>> brokersMsgPerSec() {
    var eachMsg = new HashMap<Integer, List<Double>>();

    for (Map.Entry<Integer, Map.Entry<Map.Entry<String, Integer>, Map<String, List<HasBeanObject>>>>
        entry : valueMetrics.entrySet()) {
      List<Double> sumList = new ArrayList<>();
      var outMsg =
          entry.getValue().getValue().get(KafkaMetrics.BrokerTopic.BytesOutPerSec.toString());
      var inMsg =
          entry.getValue().getValue().get(KafkaMetrics.BrokerTopic.BytesInPerSec.toString());

      var test =
          valueMetrics.values().stream().map(s -> s.getValue().values()).findAny().get().size();
      IntStream.range(0, Math.min(outMsg.size(), inMsg.size()))
          .forEach(
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

  public List<Double> avgBrokersMsgPerSec(Map<Integer, List<Double>> eachMsg) {
    return IntStream.range(0, eachMsg.values().stream().findFirst().get().size())
        .mapToDouble(i -> eachMsg.values().stream().map(s -> s.get(i)).reduce(0.0, Double::sum))
        .map(sum -> sum / eachMsg.size())
        .boxed()
        .collect(Collectors.toList());
  }

  public double standardDeviationImperative(
      Map<Integer, Double> eachMsg, double avgBrokersMsgPerSec) {
    var variance = 0.0;
    for (Map.Entry<Integer, Double> entry : eachMsg.entrySet()) {
      variance +=
          (entry.getValue() - avgBrokersMsgPerSec) * (entry.getValue() - avgBrokersMsgPerSec);
    }
    return Math.sqrt(variance / eachMsg.size());
  }

  /**
   * @param valueMetrics nodes metrics that exists in topic
   * @return the name and corresponding data of the metrics obtained by each node
   */
  public Map<Integer, Map.Entry<Map.Entry<String, Integer>, Map<String, List<HasBeanObject>>>>
      metricsNameObjects(
          HashMap<Integer, Map.Entry<Map.Entry<String, Integer>, List<HasBeanObject>>>
              valueMetrics) {
    Map<Integer, Map.Entry<Map.Entry<String, Integer>, Map<String, List<HasBeanObject>>>> result =
        new HashMap<>();
    for (Map.Entry<Integer, Map.Entry<Map.Entry<String, Integer>, List<HasBeanObject>>> entry :
        valueMetrics.entrySet()) {
      var metricsName =
          entry.getValue().getValue().stream()
              .filter(distinctByKey(b -> b.beanObject().getProperties().values()))
              .map(b -> b.beanObject().getProperties().values().stream().findAny().get())
              .distinct()
              .collect(Collectors.toList());
      var objectName =
          metricsName.stream()
              .collect(
                  Collectors.toMap(
                      Function.identity(),
                      name ->
                          entry.getValue().getValue().stream()
                              .filter(
                                  mbean ->
                                      mbean.beanObject().getProperties().values().stream()
                                          .findAny()
                                          .get()
                                          .equals(name))
                              .collect(Collectors.toList())));

      result.put(entry.getKey(), Map.entry(entry.getValue().getKey(), objectName));
    }
    return result;
  }

  private Map<Integer, Double> brokersMsgSec(int index) {
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
}
