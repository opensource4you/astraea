package org.astraea.partitioner.partitionerFactory;

import static org.astraea.partitioner.partitionerFactory.DependencyClient.addPartitioner;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.astraea.partitioner.nodeLoadMetric.BrokersWeight;
import org.astraea.partitioner.nodeLoadMetric.LoadPoisson;
import org.astraea.partitioner.nodeLoadMetric.NodeLoadClient;

/**
 * Based on the jmx metrics obtained from Kafka, it records the load status of the node over a
 * period of time. Predict the future status of each node through the poisson of the load status.
 * Finally, the result of poisson is used as the weight to perform smooth weighted RoundRobin.
 */
public class SmoothWeightPartitioner implements Partitioner {

  private NodeLoadClient nodeLoadClient;
  private DependencyManager dependencyManager;
  private int targetPartition;

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    Map<Integer, Integer> overLoadCount = null;
    var rand = new Random();
    switch (dependencyManager.currentState) {
      case Start_Dependency:
        try {
          overLoadCount = nodeLoadClient.nodesOverLoad(cluster);
        } catch (UnknownHostException e) {
          e.printStackTrace();
        }
        assert overLoadCount != null;
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
        try {
          overLoadCount = nodeLoadClient.nodesOverLoad(cluster);
        } catch (UnknownHostException e) {
          e.printStackTrace();
        }
        var loadPoisson = new LoadPoisson();
        var brokersWeight = new BrokersWeight();
        assert overLoadCount != null;
        brokersWeight.brokerHashMap(loadPoisson.allPoisson(overLoadCount));
        Map.Entry<Integer, int[]> maxWeightServer = null;

        var allWeight = brokersWeight.allNodesWeight();
        var currentBrokerHashMap = brokersWeight.brokerHashMap();

        for (Map.Entry<Integer, int[]> item : currentBrokerHashMap.entrySet()) {
          if (maxWeightServer == null || item.getValue()[1] > maxWeightServer.getValue()[1]) {
            maxWeightServer = item;
          }
          currentBrokerHashMap.put(
              item.getKey(),
              new int[] {item.getValue()[0], item.getValue()[1] + item.getValue()[0]});
        }
        assert maxWeightServer != null;
        currentBrokerHashMap.put(
            maxWeightServer.getKey(),
            new int[] {maxWeightServer.getValue()[0], maxWeightServer.getValue()[1] - allWeight});
        brokersWeight.currentBrokerHashMap(currentBrokerHashMap);

        ArrayList<Integer> partitionList = new ArrayList<>();
        for (PartitionInfo partitionInfo : cluster.partitionsForNode(maxWeightServer.getKey())) {
          partitionList.add(partitionInfo.partition());
        }
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
      for (String str : list) {
        var listAddress = Arrays.asList(str.split(":"));
        mapAddress.put(listAddress.get(0), Integer.parseInt(listAddress.get(1)));
      }
      Objects.requireNonNull(mapAddress, "You must configure jmx_servers correctly.");

      nodeLoadClient = new NodeLoadClient(mapAddress);
      dependencyManager = new DependencyManager();
      addPartitioner(configs, this);
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }

  public synchronized void initializeDependency() {
    dependencyManager.initializeDependency();
  }

  public synchronized void beginDependency() {
    dependencyManager.beginDependency();
  }

  public synchronized void finishDependency() {
    dependencyManager.finishDependency();
  }

  private static class DependencyManager {
    private volatile State currentState = State.UNINITIALIZED;

    private synchronized void initializeDependency() {
      transitionTo(State.READY);
    }

    private synchronized void beginDependency() {
      transitionTo(State.Start_Dependency);
    }

    private synchronized void finishDependency() {
      transitionTo(State.UNINITIALIZED);
    }

    private enum State {
      UNINITIALIZED,
      READY,
      Start_Dependency,
      IN_Dependency,
      FATAL_ERROR;

      private boolean isTransitionValid(
          DependencyManager.State source, DependencyManager.State target) {
        switch (target) {
          case UNINITIALIZED:
            return source == READY || source == IN_Dependency;
          case READY:
            return source == UNINITIALIZED;
          case Start_Dependency:
            return source == READY;
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
