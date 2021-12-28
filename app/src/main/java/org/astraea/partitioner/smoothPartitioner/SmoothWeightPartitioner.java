package org.astraea.partitioner.smoothPartitioner;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.astraea.Utils;
import org.astraea.partitioner.nodeLoadMetric.LoadPoisson;
import org.astraea.partitioner.nodeLoadMetric.NodeLoadClient;

/**
 * Based on the jmx metrics obtained from Kafka, it records the load status of the node over a
 * period of time. Predict the future status of each node through the poisson of the load status.
 * Finally, the result of poisson is used as the weight to perform smooth weighted RoundRobin.
 *
 * <p>SmoothWeightPartitioner also allows users to make producers send dependency data.
 *
 * <pre>{@code
 * KafkaProducer producer = new KafkaProducer(props);
 *
 * var dependencyControl = SmoothWeightPartitioner.dependencyControl(producer);
 * dependencyControl.startDependency();
 * try{
 *     producer.send();
 *     producer.send();
 *     producer.send();
 * } finally{
 *     dependencyControl.finishDependency();
 * }
 * }</pre>
 */
public class SmoothWeightPartitioner implements Partitioner, DependencyClient {

  /**
   * Record the current weight of each node according to Poisson calculation and the weight after
   * partitioner calculation.
   */
  private Map<Integer, int[]> brokerHashMap = new HashMap<>();

  private NodeLoadClient nodeLoadClient;
  private final DependencyManager dependencyManager = new DependencyManager();
  /**
   * This parameter only works in dependency mode.And it will be specified when the dependency is
   * executed for the first time, and no changes will be made afterwards.
   */
  private int targetPartition;

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    Map<Integer, Integer> overLoadCount;
    var rand = new Random();
    switch (dependencyManager.currentState) {
      case Start_Dependency:
        overLoadCount = overLoadCount(cluster);
        Objects.requireNonNull(overLoadCount, "OverLoadCount should not be null.");
        var minOverLoadNode =
            overLoadCount.entrySet().stream().min(Map.Entry.comparingByValue()).get();
        var minOverLoadPartition = cluster.partitionsForNode(minOverLoadNode.getKey());
        targetPartition =
            minOverLoadPartition.get(rand.nextInt(minOverLoadPartition.size())).partition();
        dependencyManager.transitionTo(DependencyManager.State.IN_Dependency);
        return targetPartition;
      case IN_Dependency:
        return targetPartition;
      default:
        overLoadCount = overLoadCount(cluster);
        var loadPoisson = new LoadPoisson();

        Objects.requireNonNull(overLoadCount, "OverLoadCount should not be null.");
        brokerHashMap(loadPoisson.allPoisson(overLoadCount));
        AtomicReference<Map.Entry<Integer, int[]>> maxWeightServer = new AtomicReference<>();
        var allWeight = allNodesWeight();
        var currentBrokerHashMap = brokerHashMap();
        currentBrokerHashMap.forEach(
            (nodeID, weight) -> {
              if (maxWeightServer.get() == null
                  || weight[1] > maxWeightServer.get().getValue()[1]) {
                maxWeightServer.set(Map.entry(nodeID, weight));
              }
              currentBrokerHashMap.put(nodeID, new int[] {weight[0], weight[1] + weight[0]});
            });
        Objects.requireNonNull(maxWeightServer.get(), "MaxWeightServer should not be null.");
        currentBrokerHashMap.put(
            maxWeightServer.get().getKey(),
            new int[] {
              maxWeightServer.get().getValue()[0], maxWeightServer.get().getValue()[1] - allWeight
            });
        currentBrokerHashMap(currentBrokerHashMap);

        ArrayList<Integer> partitionList = new ArrayList<>();
        cluster
            .partitionsForNode(maxWeightServer.get().getKey())
            .forEach(partitionInfo -> partitionList.add(partitionInfo.partition()));

        return partitionList.get(rand.nextInt(partitionList.size()));
    }
  }

  @Override
  public void close() {
    nodeLoadClient.close();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    try {
      var jmxAddresses =
          Objects.requireNonNull(
              configs.get("jmx_servers").toString(), "You must configure jmx_servers correctly");
      var list = Arrays.asList((jmxAddresses).split(","));
      HashMap<String, Integer> mapAddress = new HashMap<>();
      list.forEach(
          str ->
              mapAddress.put(
                  Arrays.asList(str.split(":")).get(0),
                  Integer.parseInt(Arrays.asList(str.split(":")).get(1))));
      Objects.requireNonNull(mapAddress, "You must configure jmx_servers correctly.");

      nodeLoadClient = new NodeLoadClient(mapAddress);
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }

  /** Change the weight of the node according to the current Poisson. */
  // visible for test
  public synchronized void brokerHashMap(Map<Integer, Double> poissonMap) {
    poissonMap.forEach(
        (key, value) -> {
          if (!brokerHashMap.containsKey(key)) {
            brokerHashMap.put(key, new int[] {(int) ((1 - value) * 20), 0});
          } else {
            brokerHashMap.put(key, new int[] {(int) ((1 - value) * 20), brokerHashMap.get(key)[1]});
          }
        });
  }

  private synchronized int allNodesWeight() {
    return brokerHashMap.values().stream().mapToInt(vs -> vs[0]).sum();
  }

  // visible for test
  public synchronized Map<Integer, int[]> brokerHashMap() {
    return brokerHashMap;
  }

  private synchronized void currentBrokerHashMap(Map<Integer, int[]> currentBrokerHashMap) {
    brokerHashMap = currentBrokerHashMap;
  }

  // visible for test
  public void brokerHashMapValue(Integer x, int y) {
    brokerHashMap.put(x, new int[] {0, y});
  }

  public synchronized void beginDependency() {
    dependencyManager.beginDependency();
  }

  public synchronized void finishDependency() {
    dependencyManager.finishDependency();
  }

  private Map<Integer, Integer> overLoadCount(Cluster cluster) {
    try {
      return nodeLoadClient.nodesOverLoad(cluster);
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Get the partitioner of the producer.
   *
   * @param producer kafka producer.
   * @return smoothWeightPartitioner in the producer.
   */
  public static DependencyClient dependencyControl(KafkaProducer<?, ?> producer)
      throws IllegalAccessException {
    return (DependencyClient) Utils.requireField(producer, "partitioner");
  }

  /**
   * Store the state related to the partitioner dependency and avoid wrong transitions between them.
   */
  private static class DependencyManager {
    private volatile State currentState = State.UNINITIALIZED;

    private synchronized void beginDependency() {
      transitionTo(State.Start_Dependency);
    }

    private synchronized void finishDependency() {
      transitionTo(State.UNINITIALIZED);
    }

    private enum State {
      UNINITIALIZED,
      Start_Dependency,
      IN_Dependency,
      FATAL_ERROR;

      private boolean isTransitionValid(
          DependencyManager.State source, DependencyManager.State target) {
        switch (target) {
          case UNINITIALIZED:
            return source == Start_Dependency || source == IN_Dependency;
          case Start_Dependency:
            return source == UNINITIALIZED;
          case IN_Dependency:
            return source == Start_Dependency;
          default:
            return true;
        }
      }
    }

    private void transitionTo(State target) {
      if (!currentState.isTransitionValid(currentState, target)) {
        throw new KafkaException(
            "Invalid transition attempted from state "
                + currentState.name()
                + " to state "
                + target.name());
      }
      currentState = target;
    }
  }
}
