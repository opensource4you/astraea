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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.astraea.Utils;
import org.astraea.metrics.collector.Receiver;
import org.astraea.metrics.kafka.KafkaMetrics;

/**
 * this clas is responsible for obtaining jmx metrics from BeanCollector and calculating the
 * overload status of each node through them.
 */
public class NodeLoadClient {

  private final List<Receiver> receiverList;

  private Map<Integer, Map<String, Receiver>> eachNodeIDMetrics;

  private final Map<String, Integer> currentJmxAddresses;

  private static final ReceiverFactory RECEIVER_FACTORY = new ReceiverFactory();

  private static final String regex =
      "((25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)\\.){3}" + "(25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)$";

  public NodeLoadClient(Map<String, Integer> jmxAddresses) throws IOException {
    this.receiverList = RECEIVER_FACTORY.receiversList(jmxAddresses);
    receiverList.forEach(receiver -> Utils.waitFor(() -> receiver.current().size() > 0));
    currentJmxAddresses = jmxAddresses;
  }

  /**
   * @param cluster the cluster from the partitioner
   * @return each node overLoad count in preset time
   */
  public Map<Integer, Integer> nodesOverLoad(Cluster cluster) throws UnknownHostException {
    var nodeIDReceiver = new HashMap<Integer, List<Receiver>>();
    var addresses =
        cluster.nodes().stream()
            .collect(Collectors.toMap(Node::id, node -> Map.entry(node.host(), node.port())));

    for (Map.Entry<Integer, Map.Entry<String, Integer>> hostPort : addresses.entrySet()) {
      List<Receiver> matchingNode;
      if (!hostPort.getValue().getKey().matches(regex)) {
        var host = InetAddress.getByName(hostPort.getValue().getKey()).toString().split("/")[1];
        matchingNode =
            receiverList.stream()
                .filter(
                    receiver ->
                        (Objects.equals(nodeIDReceiver.size(), 0)
                                || (nodeIDReceiver.values().stream()
                                        .map(
                                            n ->
                                                Objects.equals(
                                                    n.stream().findAny().get().host(), host))
                                        .anyMatch(n -> n.equals(true))
                                    && nodeIDReceiver.values().stream()
                                        .map(
                                            n ->
                                                !Objects.equals(
                                                    n.stream().findAny().get().port(),
                                                    hostPort.getValue().getValue()))
                                        .anyMatch(n -> n.equals(true))))
                            && Objects.equals(receiver.host(), host))
                .collect(Collectors.toList());
      } else {
        matchingNode =
            receiverList.stream()
                .filter(
                    receiver ->
                        (Objects.equals(nodeIDReceiver.size(), 0)
                                || (nodeIDReceiver.values().stream()
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
                                        .anyMatch(n -> n.equals(true))))
                            && Objects.equals(receiver.host(), hostPort.getValue().getKey()))
                .collect(Collectors.toList());
      }

      if (matchingNode.size() > 0) {
        if (!nodeIDReceiver.containsKey(hostPort.getKey()))
          nodeIDReceiver.put(hostPort.getKey(), matchingNode);
        else matchingNode.forEach(node -> nodeIDReceiver.get(hostPort.getKey()).add(node));
      }
    }

    eachNodeIDMetrics = metricsNameForReceiver(nodeIDReceiver);
    var eachBrokerMsgPerSec = brokersMsgPerSec();
    var overLoadCount = new HashMap<Integer, Integer>();
    eachBrokerMsgPerSec.keySet().forEach(e -> overLoadCount.put(e, 0));
    var i = 0;
    var minRecordSec =
        eachBrokerMsgPerSec.values().stream()
            .map(n -> n.size())
            .min(Comparator.comparing(Integer::intValue))
            .get();
    while (i < minRecordSec) {
      var avg = avgMsgSec(i, eachBrokerMsgPerSec);
      var eachMsg = brokersMsgSec(i, eachBrokerMsgPerSec);
      var standardDeviation = standardDeviationImperative(eachMsg, avg);
      eachMsg.forEach(
          (nodeID, msg) -> {
            if (msg > (avg + standardDeviation)) {
              overLoadCount.put(nodeID, overLoadCount.get(nodeID) + 1);
            }
          });
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
      var outMsgBean = outMsg.current();
      var inMsgBean = inMsg.current();
      IntStream.range(0, Math.min(outMsgBean.size(), inMsgBean.size()))
          .forEach(
              i ->
                  sumList.add(
                      sum(
                          Double.parseDouble(
                              outMsgBean
                                  .get(i)
                                  .beanObject()
                                  .getAttributes()
                                  .get("MeanRate")
                                  .toString()),
                          Double.parseDouble(
                              inMsgBean
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
    var variance = new AtomicReference<>(0.0);
    eachMsg.forEach(
        (nodeID, msg) ->
            variance.updateAndGet(
                v -> v + (msg - avgBrokersMsgPerSec) * (msg - avgBrokersMsgPerSec)));
    return Math.sqrt(variance.get() / eachMsg.size());
  }

  /**
   * @param nodeIDReceiver nodes metrics that exists in topic
   * @return the name and corresponding data of the metrics obtained by each node
   */
  public Map<Integer, Map<String, Receiver>> metricsNameForReceiver(
      HashMap<Integer, List<Receiver>> nodeIDReceiver) {

    var result = new HashMap<Integer, Map<String, Receiver>>();

    nodeIDReceiver.forEach(
        (nodeID, receivers) -> {
          result.put(
              nodeID,
              receivers.stream()
                  .map(
                      receiver ->
                          receiver.current().stream()
                              .map(
                                  bean ->
                                      bean.beanObject().getProperties().values().stream()
                                          .findAny()
                                          .get())
                              .findAny()
                              .get())
                  .collect(Collectors.toList())
                  .stream()
                  .collect(
                      Collectors.toMap(
                          Function.identity(),
                          name ->
                              receivers.stream()
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
                                  .get())));
        });
    return result;
  }

  private Map<Integer, Double> brokersMsgSec(int index, Map<Integer, List<Double>> eachMsg) {
    return eachMsg.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get(index)));
  }

  private double avgMsgSec(int index, Map<Integer, List<Double>> eachMsg) {
    return avgBrokersMsgPerSec(eachMsg).get(index);
  }

  public void close() {
    RECEIVER_FACTORY.close(currentJmxAddresses);
  }
}
