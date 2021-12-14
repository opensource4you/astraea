package org.astraea.partitioner.nodeLoadMetric;

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
import org.apache.kafka.common.Cluster;
import org.astraea.Utils;
import org.astraea.metrics.collector.Receiver;
import org.astraea.metrics.kafka.KafkaMetrics;

/**
 * this clas is responsible for obtaining jmx metrics from BeanCollector and calculating the
 * overload status of each node through them.
 */
public class NodeLoadClient {

  private List<Receiver> receiverList;

  private Map<Integer, Map<String, Receiver>> eachNodeIDMetrics;

  private static final BeanCollectorFactory FACTORY = new BeanCollectorFactory();

  public NodeLoadClient(Map<String, Integer> jmxAddresses) throws IOException {

    this.receiverList = FACTORY.receiversList(jmxAddresses);
    receiverList.forEach(receiver -> Utils.waitFor(() -> receiver.current().size() > 0));
    receiverList.forEach(receiver -> System.out.println(receiver.current().size()));
  }

  /**
   * @param cluster the cluster from the partitioner
   * @return each node overLoad count in preset time
   */
  public Map<Integer, Integer> nodesOverLoad(Cluster cluster) throws UnknownHostException {
    var nodeIDReceiver = new HashMap<Integer, List<Receiver>>();
    var addresses =
        cluster.nodes().stream()
            .collect(
                Collectors.toMap(node -> node.id(), node -> Map.entry(node.host(), node.port())));

    for (Receiver receiver : receiverList) {
      receiver.host();
    }

    for (Map.Entry<Integer, Map.Entry<String, Integer>> hostPort : addresses.entrySet()) {
      Receiver matchingNode;
      String regex =
          "((25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)\\.){3}"
              + "(25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)$";
      if (!hostPort.getValue().getKey().matches(regex)) {
        var localhost =
            InetAddress.getByName(hostPort.getValue().getKey()).toString().split("/")[1];
        matchingNode =
            receiverList.stream()
                .filter(
                    receiver ->
                        ((nodeIDReceiver.values().stream()
                                        .map(
                                            n ->
                                                Objects.equals(
                                                    n.stream().findAny().get().host(), localhost))
                                        .anyMatch(n -> n.equals(true))
                                    && nodeIDReceiver.values().stream()
                                        .map(
                                            n ->
                                                !Objects.equals(
                                                    n.stream().findAny().get().port(),
                                                    hostPort.getValue().getValue()))
                                        .anyMatch(n -> n.equals(true)))
                                || Objects.equals(nodeIDReceiver.size(), 0))
                            && Objects.equals(receiver.host(), localhost))
                .findAny()
                .orElse(null);
      } else {
        matchingNode =
            receiverList.stream()
                .filter(
                    receiver ->
                        ((nodeIDReceiver.values().stream()
                                        .map(
                                            n ->
                                                Objects.equals(
                                                    n.stream().findAny().get().host(),
                                                    hostPort.getValue().getKey()))
                                        .anyMatch(n -> n.equals(true))
                                    && nodeIDReceiver.values().stream()
                                        .map(
                                            n ->
                                                Objects.equals(
                                                    n.stream().findAny().get().port(),
                                                    hostPort.getValue().getValue()))
                                        .anyMatch(n -> n.equals(true)))
                                || Objects.equals(nodeIDReceiver.size(), 0))
                            && Objects.equals(receiver.host(), hostPort.getValue().getKey()))
                .findAny()
                .orElse(null);
      }

      var addressEntrySet = new HashMap<String, Integer>();

      if (matchingNode != null) {
        addressEntrySet.put(matchingNode.host(), matchingNode.port());
        if (!nodeIDReceiver.containsKey(addressEntrySet.entrySet().stream().findAny().get()))
          nodeIDReceiver.put(hostPort.getKey(), List.of(matchingNode));
      }
    }

    eachNodeIDMetrics = metricsNameObjects(nodeIDReceiver);
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

    for (Map.Entry<Integer, Map<String, Receiver>> entry : eachNodeIDMetrics.entrySet()) {
      List<Double> sumList = new ArrayList<>();
      var outMsg = entry.getValue().get(KafkaMetrics.BrokerTopic.BytesOutPerSec.toString());
      var inMsg = entry.getValue().get(KafkaMetrics.BrokerTopic.BytesInPerSec.toString());

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
   * @param nodeIDReceiver nodes metrics that exists in topic
   * @return the name and corresponding data of the metrics obtained by each node
   */
  public Map<Integer, Map<String, Receiver>> metricsNameObjects(
      HashMap<Integer, List<Receiver>> nodeIDReceiver) {

    Map<Integer, Map<String, Receiver>> result = new HashMap<>();

    for (Map.Entry<Integer, List<Receiver>> entry : nodeIDReceiver.entrySet()) {
      var metricsName =
          entry.getValue().stream()
              .filter(
                  distinctByKey(
                      b ->
                          b.current().stream()
                              .findAny()
                              .get()
                              .beanObject()
                              .getProperties()
                              .values()))
              .map(
                  b ->
                      b.current().stream()
                          .findAny()
                          .get()
                          .beanObject()
                          .getProperties()
                          .values()
                          .stream()
                          .findAny()
                          .get())
              .distinct()
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
                                      mbean.current().stream()
                                          .findAny()
                                          .get()
                                          .beanObject()
                                          .getProperties()
                                          .values()
                                          .stream()
                                          .findAny()
                                          .get()
                                          .equals(name))
                              .findAny()
                              .get()));

      result.put(entry.getKey(), objectName);
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
    FACTORY.close();
  }

  static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
    Map<Object, Boolean> seen = new ConcurrentHashMap<>();
    return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
  }
}
