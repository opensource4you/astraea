package org.astraea.partitioner.nodeLoadMetric;

import static org.astraea.Utils.overSecond;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.astraea.Utils;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.NodeInfo;
import org.astraea.metrics.collector.Receiver;
import org.astraea.metrics.java.HasJvmMemory;
import org.astraea.metrics.kafka.KafkaMetrics;

/**
 * this clas is responsible for obtaining jmx metrics from BeanCollector and calculating the
 * overload status of each node through them.
 */
public class NodeLoadClient {

  private final Lock lock = new ReentrantLock();
  private final Optional<Integer> jmxPortDefault;
  private static final ReceiverFactory RECEIVER_FACTORY = new ReceiverFactory();
  private long lastTime = -1;
  // visible for testing
  List<Broker> brokers;
  private Map<Integer, Integer> currentLoadNode;
  private int referenceBrokerID;
  private int totalInput;
  private int totalOutput;
  private Map<Integer, Integer> jmxOfBrokerID;
  private Map<String, Integer> jmxAddress;

  private static final String regex =
      "((25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)\\.){3}" + "(25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)$";

  public NodeLoadClient(Map<Integer, Integer> jmxAddresses, int jmxPortDefault) {
    if (jmxPortDefault != -1) this.jmxPortDefault = Optional.of(jmxPortDefault);
    else this.jmxPortDefault = Optional.empty();
    jmxOfBrokerID = jmxAddresses;
  }

  /**
   * When the interval between getting the load twice exceeds one second, update the load and
   * return, otherwise directly return to the existing load.
   *
   * @param cluster from partitioner
   * @return each node load count in preset time
   */
  public Map<Integer, Integer> loadSituation(ClusterInfo cluster) {
    if (overSecond(lastTime, 1) && lock.tryLock()) {
      try {
        lock.lock();
        nodesOverLoad(cluster);
      } finally {
        lock.unlock();
        lastTime = System.currentTimeMillis();
      }
    }
    Utils.waitFor(() -> currentLoadNode != null);
    return currentLoadNode;
  }

  Map<String, Integer> jmxAddress(ClusterInfo cluster) {
    var iterator = jmxOfBrokerID.keySet().iterator();
    var currentJmxAddresses = new TreeMap<String, Integer>();
    var idHost = cluster.nodes().stream().collect(Collectors.toMap(NodeInfo::id, NodeInfo::host));
    while (iterator.hasNext()) {
      var id = iterator.next();
      if (idHost.containsKey(id)) {
        currentJmxAddresses.put(ipAddress(idHost.get(id)), jmxOfBrokerID.get(id));
        idHost.remove(id);
      }
    }

    if (idHost.size() > 0) {
      jmxPortDefault.ifPresentOrElse(
          port -> idHost.forEach((key, value) -> currentJmxAddresses.put(ipAddress(value), port)),
          () -> {
            var notFindJMX = new ArrayList<>(idHost.keySet());
            throw new RuntimeException(notFindJMX + "jmx servers not found.");
          });
    }
    return currentJmxAddresses;
  }

  /** @param cluster the cluster from the partitioner */
  private void nodesOverLoad(ClusterInfo cluster) {
    if (lastTime == -1) {
      var nodes = cluster.nodes();
      brokers =
          nodes.stream()
              .map(node -> new Broker(node.id(), node.host(), node.port()))
              .collect(Collectors.toList());
      referenceBrokerID = brokers.stream().findFirst().get().brokerID;
      jmxAddress = jmxAddress(cluster);
      var receiverList = RECEIVER_FACTORY.receiversList(jmxAddress);
      receiverList.forEach(receiver -> Utils.waitFor(() -> receiver.current().size() > 0));
      var nodeIDReceiver = new HashMap<Integer, List<Receiver>>();
      receiverList.forEach(
          receiver -> {
            var brokersID = brokerIDOfReceiver(receiver.host());
            brokersID.forEach(
                brokerID -> {
                  if (nodeIDReceiver.containsKey(brokerID)) {
                    nodeIDReceiver.get(brokerID).add(receiver);
                  } else {
                    nodeIDReceiver.put(brokerID, new ArrayList<>(List.of(receiver)));
                  }
                });
          });
      metricsNameForReceiver(nodeIDReceiver);
    }

    brokersMsgPerSec();
    totalInput = totalInput();
    totalOutput = totalOutput();

    AtomicReference<Double> totalBrokerSituation = new AtomicReference<>(0.0);
    brokers.forEach(
        broker -> {
          broker.maxThoughPut();
          broker.inputNormalized(totalInput);
          broker.outputNormalized(totalOutput);
          broker.thoughPutComparison(findBroker(referenceBrokerID).maxThoughPut);
          broker.brokerSituation();
          totalBrokerSituation.updateAndGet(v -> v + broker.brokerSituation);
        });

    brokers.forEach(broker -> broker.brokerSituationNormalized(totalBrokerSituation.get()));

    brokers.forEach(
        broker ->
            broker.load(
                brokerLoad(broker.brokerSituation, totalBrokerSituation.get() / brokers.size())));
    this.currentLoadNode =
        brokers.stream().collect(Collectors.toMap(broker -> broker.brokerID, Broker::totalLoad));
  }

  private void brokersMsgPerSec() {
    brokers.forEach(
        broker -> {
          var currentOutput =
              Long.parseLong(
                  broker
                      .metrics
                      .get(KafkaMetrics.BrokerTopic.BytesInPerSec.name())
                      .current()
                      .iterator()
                      .next()
                      .beanObject()
                      .getAttributes()
                      .get("Count")
                      .toString());
          var currentInput =
              Long.parseLong(
                  broker
                      .metrics
                      .get(KafkaMetrics.BrokerTopic.BytesOutPerSec.name())
                      .current()
                      .iterator()
                      .next()
                      .beanObject()
                      .getAttributes()
                      .get("Count")
                      .toString());
          if (broker.lastInput == -1) {
            broker.lastInput = currentInput;
            broker.lastOutPut = currentOutput;
          }

          broker.output =
              Integer.parseInt(String.valueOf((currentOutput - broker.lastOutPut) / 1024));
          broker.lastOutPut = currentOutput;
          broker.input = Integer.parseInt(String.valueOf((currentInput - broker.lastInput) / 1024));
          broker.lastInput = currentInput;
          Objects.requireNonNull(broker.metrics.get("Memory"));
          broker.jvm =
              (HasJvmMemory)
                  broker.metrics.get("Memory").current().stream().findAny().orElse(broker.jvm);
        });
  }

  // TODO Regarding countermeasures against cluster conditions
  public double standardDeviationImperative(double avgBrokersSituation) {
    var variance = new AtomicReference<>(0.0);
    brokers.forEach(
        broker ->
            variance.updateAndGet(
                v ->
                    v
                        + (broker.brokerSituationNormalized - avgBrokersSituation)
                            * (broker.brokerSituationNormalized - avgBrokersSituation)));
    return Math.sqrt(variance.get() / brokers.size());
  }

  /** @param brokerIDReceiver nodes metrics that exists in topic */
  private void metricsNameForReceiver(HashMap<Integer, List<Receiver>> brokerIDReceiver) {
    brokerIDReceiver.forEach(
        (brokerID, receivers) ->
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
                        findBroker(brokerID)
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

  private Broker findBroker(int brokerID) {
    return brokers.stream().filter(broker -> broker.brokerID == brokerID).findAny().get();
  }

  private String ipAddress(String host) {
    var correctHost = "-1.-1.-1.-1";
    if (notIPAddress(host)) {
      try {
        correctHost = String.valueOf(InetAddress.getByName(host)).split("/")[1];
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
    } else {
      correctHost = host;
    }
    return correctHost;
  }

  private boolean notIPAddress(String host) {
    return !host.matches(regex);
  }

  public void close() {
    RECEIVER_FACTORY.close(jmxAddress);
  }

  private List<Integer> brokerIDOfReceiver(String host) {
    return brokers.stream()
        .filter(broker -> Objects.equals(ipAddress(broker.host), host))
        .map(broker -> broker.brokerID)
        .collect(Collectors.toList());
  }

  private int totalInput() {
    return brokers.stream().map(broker -> broker.input).reduce(0, Integer::sum);
  }

  private int totalOutput() {
    return brokers.stream().map(broker -> broker.output).reduce(0, Integer::sum);
  }

  int brokerLoad(double brokerSituation, double avg) {
    if (brokerSituation <= (1 - 0.5) * avg) {
      return 0;
    } else if (brokerSituation >= (1 + 0.5) * avg) {
      return 2;
    } else {
      return 1;
    }
  }

  public double thoughPutComparison(int brokerID) {
    return findBroker(brokerID).thoughPutComparison;
  }

  public double memoryUsage(int brokerID) {
    return findBroker(brokerID).memoryUsage();
  }

  static class Broker {
    private final int brokerID;
    private final String host;
    private final int port;
    private int count = 0;
    private final ConcurrentMap<Integer, Integer> load = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Receiver> metrics = new ConcurrentHashMap<>();
    private int input;
    private long lastInput = -1;
    private int output;
    private long lastOutPut = -1;
    private double maxThoughPut;
    private double brokerSituation;
    private double thoughPutComparison;
    private double inputNormalized;
    private double outputNormalized;
    private double brokerSituationNormalized;
    private HasJvmMemory jvm;
    private static final double inputWeights = 0.5;
    private static final double outputWeights = 0.5;

    // visible of test
    Broker(int brokerID, String host, int port) {
      this.brokerID = brokerID;
      this.host = host;
      this.port = port;
    }

    private int totalLoad() {
      return load.values().stream().reduce(0, Integer::sum);
    }

    private void load(Integer currentLoad) {
      load.put(count % 10, currentLoad);
      count++;
    }

    private void brokerSituationNormalized(double totalSituation) {
      this.brokerSituationNormalized = brokerSituation / totalSituation;
    }

    private void inputNormalized(int brokersInput) {
      this.inputNormalized = (input + 1.0) / (brokersInput + 1);
    }

    private void outputNormalized(int brokersOutput) {
      this.outputNormalized = (output + 1.0) / (brokersOutput + 1);
    }

    private void brokerSituation() {
      this.brokerSituation =
          (inputNormalized * inputWeights + outputNormalized * outputWeights) / thoughPutComparison;
    }

    private void maxThoughPut() {
      this.maxThoughPut = Math.max(maxThoughPut, input + output);
    }

    // TODO Algorithms adapted to computers of different specifications
    private void thoughPutComparison(double referenceThoughPut) {
      if (count >= 20) {
        this.thoughPutComparison = 1;
      } else if (count >= 10) {
        this.thoughPutComparison = 1;
      } else if (count >= 0) {
        this.thoughPutComparison = 1;
      }
    }

    private double memoryUsage() {
      if (count < 10) {
        return 0.1;
      } else {
        return (jvm.heapMemoryUsage().getUsed() + 0.0) / (jvm.heapMemoryUsage().getMax() + 1);
      }
    }
  }
}
