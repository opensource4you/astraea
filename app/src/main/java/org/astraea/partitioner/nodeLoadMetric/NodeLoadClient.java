package org.astraea.partitioner.nodeLoadMetric;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.astraea.Utils;
import org.astraea.metrics.collector.Receiver;
import org.astraea.metrics.java.HasJvmMemory;
import org.astraea.metrics.kafka.KafkaMetrics;

/**
 * this clas is responsible for obtaining jmx metrics from BeanCollector and calculating the
 * overload status of each node through them.
 */
public class NodeLoadClient {

  private final List<Receiver> receiverList;
  private final Map<String, Integer> currentJmxAddresses;
  private static final ReceiverFactory RECEIVER_FACTORY = new ReceiverFactory();
  private long lastTime = -1;
  // visible for testing
  Map<Integer, Broker> brokers;
  private Map<Integer, Integer> currentLoadNode;
  private int referenceBrokerID;
  private Broker referenceBroker;

  private static final String regex =
      "((25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)\\.){3}" + "(25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)$";

  public NodeLoadClient(Map<String, Integer> jmxAddresses) throws IOException {
    this.receiverList = RECEIVER_FACTORY.receiversList(jmxAddresses);
    receiverList.forEach(receiver -> Utils.waitFor(() -> receiver.current().size() > 0));
    currentJmxAddresses = jmxAddresses;
  }

  /**
   * When the interval between getting the load twice exceeds one second, update the load and
   * return, otherwise directly return to the existing load.
   *
   * @param cluster from partitioner
   * @return each node load count in preset time
   * @throws UnknownHostException
   */
  public Map<Integer, Integer> loadSituation(Cluster cluster) throws UnknownHostException {
    if (overOneSecond()) {
      nodesOverLoad(cluster);
    }
    return currentLoadNode;
  }

  /**
   * @param cluster the cluster from the partitioner
   * @return each node load count in preset time
   */
  private void nodesOverLoad(Cluster cluster) {
    var nodes = cluster.nodes();
    if (lastTime == -1) {
      brokers = nodes.stream().collect(Collectors.toMap(Node::id, node -> new Broker()));
      referenceBrokerID = brokers.keySet().stream().findFirst().get();
      referenceBroker = brokers.get(referenceBrokerID);
    }
    var nodeIDReceiver = new HashMap<Integer, List<Receiver>>();
    var addresses =
        nodes.stream()
            .collect(Collectors.toMap(Node::id, node -> Map.entry(node.host(), node.port())));

    addresses
        .entrySet()
        .forEach(
            hostPort -> {
              List<Receiver> matchingNode;
              var host = ipAddress(hostPort);
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

              if (matchingNode.size() > 0) {
                if (!nodeIDReceiver.containsKey(hostPort.getKey()))
                  nodeIDReceiver.put(hostPort.getKey(), matchingNode);
                else matchingNode.forEach(node -> nodeIDReceiver.get(hostPort.getKey()).add(node));
              }
            });

    metricsNameForReceiver(nodeIDReceiver);
    brokersMsgPerSec();

    var totalInput = brokers.values().stream().map(broker -> broker.input).reduce(0.0, Double::sum);
    var totalOutput =
        brokers.values().stream().map(broker -> broker.output).reduce(0.0, Double::sum);

    AtomicReference<Double> totalBrokerSituation = new AtomicReference<>(0.0);
    brokers
        .values()
        .forEach(
            broker -> {
              broker.maxThoughPut();
              broker.inputNormalized(totalInput);
              broker.outputNormalized(totalOutput);
              broker.thoughPutComparison(referenceBroker.maxThoughPut);
              broker.brokerSituation();
              totalBrokerSituation.updateAndGet(v -> v + broker.brokerSituation);
            });

    var totalSituation = totalBrokerSituation.get();

    brokers
        .values()
        .forEach(
            broker -> {
              broker.brokerSituationNormalized(totalSituation);
            });

    var avg = totalSituation / brokers.size();

    // TODO Regarding countermeasures against cluster conditions
    var standardDeviation = standardDeviationImperative(avg);

    brokers.values().forEach(broker -> broker.load(brokerLoad(broker.brokerSituation, avg)));
    currentLoadNode =
        brokers.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().totalLoad()));
    lastTime = System.currentTimeMillis();
  }

  private void brokersMsgPerSec() {
    brokers.forEach(
        (key, broker) -> {
          broker.output =
              Double.parseDouble(
                  broker
                      .metrics
                      .get(KafkaMetrics.BrokerTopic.BytesOutPerSec.toString())
                      .current()
                      .get(0)
                      .beanObject()
                      .getAttributes()
                      .get("MeanRate")
                      .toString());
          broker.input =
              Double.parseDouble(
                  broker
                      .metrics
                      .get(KafkaMetrics.BrokerTopic.BytesInPerSec.toString())
                      .current()
                      .get(0)
                      .beanObject()
                      .getAttributes()
                      .get("MeanRate")
                      .toString());
        });
  }

  public double standardDeviationImperative(double avgBrokersSituation) {
    var variance = new AtomicReference<>(0.0);
    brokers
        .values()
        .forEach(
            broker ->
                variance.updateAndGet(
                    v ->
                        v
                            + (broker.brokerSituationNormalized - avgBrokersSituation)
                                * (broker.brokerSituationNormalized - avgBrokersSituation)));
    return Math.sqrt(variance.get() / brokers.size());
  }

  /**
   * @param nodeIDReceiver nodes metrics that exists in topic
   * @return the name and corresponding data of the metrics obtained by each node
   */
  private void metricsNameForReceiver(HashMap<Integer, List<Receiver>> nodeIDReceiver) {
    nodeIDReceiver.forEach(
        (nodeID, receivers) ->
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
                .forEach(
                    name ->
                        brokers
                            .get(nodeID)
                            .metrics
                            .put(
                                name,
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
  }

  private String ipAddress(Map.Entry<Integer, Map.Entry<String, Integer>> hostPort) {
    var host = "-1.-1.-1.-1";
    if (notIPAddress(hostPort)) {
      try {
        host = InetAddress.getByName(hostPort.getValue().getKey()).toString().split("/")[1];
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
    } else {
      host = hostPort.getValue().getKey();
    }
    return host;
  }

  private boolean notIPAddress(Map.Entry<Integer, Map.Entry<String, Integer>> hostPort) {
    return !hostPort.getValue().getKey().matches(regex);
  }

  private boolean overOneSecond() {
    return lastTime + Duration.ofSeconds(1).toMillis() <= System.currentTimeMillis();
  }

  public void close() {
    RECEIVER_FACTORY.close(currentJmxAddresses);
  }

  // visible of test
  int brokerLoad(double brokerSituation, double avg) {
    var initialization = 5;
    var loadDeviationRate = (brokerSituation - avg) / avg;
    var load = (int) Math.round(initialization + loadDeviationRate * 10);
    if (load > 10) load = 10;
    else if (load < 0) load = 0;
    return load;
  }

  public double thoughPutComparison(int brokerID) {
    return brokers.get(brokerID).thoughPutComparison;
  }

  public double memoryUsage(int brokerID) {
    return brokers.get(brokerID).memoryUsage();
  }

  // visible of test
  static class Broker {
    private int count = 0;
    private Map<Integer, Integer> load = new HashMap<>();
    private Map<String, Receiver> metrics = new HashMap<>();
    private double input;
    private double output;
    private double maxThoughPut;
    private double brokerSituation;
    private double thoughPutComparison;
    private double inputNormalized;
    private double outputNormalized;
    private double brokerSituationNormalized;
    private static final double inputWeights = 0.5;
    private static final double outputWeights = 0.5;

    private int totalLoad() {
      return load.values().stream().reduce(0, Integer::sum);
    }

    private void load(Integer currentLoad) {
      this.load.put(count % 10, currentLoad);
      count++;
    }

    private void brokerSituationNormalized(double totalSituation) {
      this.brokerSituationNormalized = brokerSituation / totalSituation;
    }

    private void inputNormalized(double brokersInput) {
      this.inputNormalized = (input + 1) / (brokersInput + 1);
    }

    private void outputNormalized(double brokersOutput) {
      this.outputNormalized = (output + 1) / (brokersOutput + 1);
    }

    private void brokerSituation() {
      this.brokerSituation =
          (inputNormalized * inputWeights + outputNormalized * outputWeights) / thoughPutComparison;
    }

    private void maxThoughPut() {
      this.maxThoughPut = Math.max(maxThoughPut, input + output);
    }

    private void thoughPutComparison(double referenceThoughPut) {
      if (count >= 20) {
        this.thoughPutComparison =
            (maxThoughPut - referenceThoughPut) / (referenceThoughPut + 1) + 1;
      } else if (count >= 10) {
        this.thoughPutComparison =
            (maxThoughPut - referenceThoughPut) / (referenceThoughPut + 1) / 2 + 1;
      } else if (count >= 0) {
        this.thoughPutComparison = 1;
      }
    }

    private double memoryUsage() {
      var jvm = (HasJvmMemory) metrics.get("Memory").current().stream().findAny().get();
      return (jvm.heapMemoryUsage().getUsed() + 0.0) / (jvm.heapMemoryUsage().getMax() + 1);
    }
  }
}
