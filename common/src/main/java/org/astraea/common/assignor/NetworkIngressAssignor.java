/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common.assignor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.DataRate;
import org.astraea.common.Utils;
import org.astraea.common.admin.BrokerTopic;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.cost.NoSufficientMetricsException;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.broker.HasRate;
import org.astraea.common.metrics.broker.ServerMetrics;

public class NetworkIngressAssignor extends Assignor {

  @Override
  protected Map<String, List<TopicPartition>> assign(
      Map<String, org.astraea.common.assignor.Subscription> subscriptions,
      ClusterInfo clusterInfo) {
    var consumers = subscriptions.keySet();
    var subscribedTopics = topics(subscriptions);
    // 1. check unregister node. if there are unregister nodes, register them
    registerUnregisterNode(clusterInfo);
    // wait for clusterBean
    Utils.waitFor(
        () -> !metricCollector.clusterBean().all().isEmpty(), Duration.ofSeconds(maxWaitBean));
    var clusterBean = metricCollector.clusterBean();

    // 2. get the network cost of all subscribed topic
    var networkCost =
        costFunction
            .partitionCost(ClusterInfo.masked(clusterInfo, subscribedTopics::contains), clusterBean)
            .value();
    var costPerBroker = costPerBroker(clusterInfo, subscribedTopics, networkCost);
    var intervalPerBroker = convertTrafficToCost(clusterInfo, clusterBean, costPerBroker);
    return greedyAssign(costPerBroker, consumers, intervalPerBroker);
  }

  /**
   * register unregistered nodes if present. if we didn't register unregistered nodes, we would miss
   * the beanObjects from the nodes
   *
   * @param clusterInfo Currently cluster information.
   */
  private void registerUnregisterNode(ClusterInfo clusterInfo) {
    var unregister = checkUnregister(clusterInfo.nodes());
    if (!unregister.isEmpty()) registerJMX(unregister);
  }

  /**
   * perform assign algorithm to get balanced assignment and ensure that 1. each consumer would
   * receive the cost that are as close as possible to each other. 2. similar loads within a node
   * would be assigned to the same consumer.
   *
   * @param costs the tp and their cost within a node
   * @param consumers consumers' name
   * @return the assignment
   */
  Map<String, List<TopicPartition>> greedyAssign(
      Map<Integer, Map<TopicPartition, Double>> costs,
      Set<String> consumers,
      Map<Integer, Double> limitedPerBroker) {
    // initial
    var assignment = new HashMap<String, List<TopicPartition>>();
    for (var consumer : consumers) {
      assignment.put(consumer, new ArrayList<>());
    }
    var costPerConsumer =
        assignment.keySet().stream()
            .map(c -> Map.entry(c, (double) 0))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    costs
        .entrySet()
        .forEach(
            costPerBroker -> {
              if (costPerBroker.getValue().values().stream().mapToDouble(x -> x).sum() == 0) {
                // if there are no cost, round-robin assign per node
                var iter = consumers.iterator();
                for (var tp : costPerBroker.getValue().keySet()) {
                  assignment.get(iter.next()).add(tp);
                  if (!iter.hasNext()) iter = consumers.iterator();
                }
              } else {
                var sortedCost = new LinkedHashMap<TopicPartition, Double>();
                costPerBroker.getValue().entrySet().stream()
                    .sorted(Map.Entry.comparingByValue())
                    .forEach(entry -> sortedCost.put(entry.getKey(), entry.getValue()));
                var tmpCostPerConsumer = new HashMap<>(costPerConsumer);
                Supplier<String> largestCostConsumer =
                    () ->
                        Collections.max(tmpCostPerConsumer.entrySet(), Map.Entry.comparingByValue())
                            .getKey();
                var consumer = largestCostConsumer.get();
                var lastValue = Collections.min(sortedCost.values());

                for (var e : sortedCost.entrySet()) {
                  var tp = e.getKey();
                  var cost = e.getValue();

                  if (cost - lastValue > limitedPerBroker.get(costPerBroker.getKey())) {
                    tmpCostPerConsumer.remove(consumer);
                    consumer = largestCostConsumer.get();
                  }

                  assignment.get(consumer).add(tp);
                  costPerConsumer.computeIfPresent(consumer, (ignore, c) -> c + cost);
                  lastValue = cost;
                }
              }
            });
    return assignment;
  }

  Map<Integer, Map<TopicPartition, Double>> costPerBroker(
      ClusterInfo clusterInfo, Set<String> topics, Map<TopicPartition, Double> cost) {
    return clusterInfo
        .replicaStream()
        .filter(Replica::isLeader)
        .filter(Replica::isOnline)
        .filter(replica -> topics.contains(replica.topic()))
        .collect(Collectors.groupingBy(replica -> replica.nodeInfo().id()))
        .entrySet()
        .stream()
        .map(
            e ->
                Map.entry(
                    e.getKey(),
                    e.getValue().stream()
                        .map(
                            replica ->
                                Map.entry(
                                    replica.topicPartition(), cost.get(replica.topicPartition())))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
  // visible for test
  /**
   * For all nodes, calculate the cost of given traffic. The assignor would use the cost to produce
   * the assignment.
   *
   * @param clusterInfo the clusterInfo
   * @param clusterBean the clusterBean
   * @param tpCostPerBroker the partition cost of every broker
   * @return the Map from broker id to the cost of given traffic
   */
  Map<Integer, Double> convertTrafficToCost(
      ClusterInfo clusterInfo,
      ClusterBean clusterBean,
      Map<Integer, Map<TopicPartition, Double>> tpCostPerBroker) {
    var interval = DataRate.MiB.of(maxTrafficMiBInterval).perSecond().byteRate();
    var partitionsTraffic =
        replicaLeaderLocation(clusterInfo).entrySet().stream()
            .flatMap(
                e -> {
                  var bt = e.getKey();
                  var totalReplicaSize = e.getValue().stream().mapToLong(Replica::size).sum();
                  var totalShare =
                      (double)
                          clusterBean
                              .brokerTopicMetrics(bt, ServerMetrics.Topic.Meter.class)
                              .filter(
                                  bean -> bean.type().equals(ServerMetrics.Topic.BYTES_IN_PER_SEC))
                              .max(Comparator.comparingLong(HasBeanObject::createdTimestamp))
                              .map(HasRate::fifteenMinuteRate)
                              .orElse(0.0);

                  if (Double.isNaN(totalShare) || totalShare < 0.0 || totalReplicaSize < 0) {
                    throw new NoSufficientMetricsException(
                        costFunction,
                        Duration.ofSeconds(1),
                        "no enough metric to calculate traffic");
                  }
                  var calculateShare =
                      (Function<Replica, Long>)
                          (replica) ->
                              totalReplicaSize > 0
                                  ? (long) ((totalShare * replica.size()) / totalReplicaSize)
                                  : 0L;
                  return e.getValue().stream()
                      .map(r -> Map.entry(r.topicPartition(), calculateShare.apply(r)));
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    return tpCostPerBroker.entrySet().stream()
        .map(
            e -> {
              var tpCost =
                  e.getValue().entrySet().stream()
                      .filter(entry -> entry.getValue() > 0.0)
                      .findFirst()
                      .get();
              var traffic = partitionsTraffic.get(tpCost.getKey());
              var normalizedCost = tpCost.getValue();

              var result = normalizedCost / (traffic / interval);
              return Map.entry(e.getKey(), result);
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map<BrokerTopic, List<Replica>> replicaLeaderLocation(ClusterInfo clusterInfo) {
    return clusterInfo
        .replicaStream()
        .filter(Replica::isLeader)
        .filter(Replica::isOnline)
        .map(
            replica -> Map.entry(BrokerTopic.of(replica.nodeInfo().id(), replica.topic()), replica))
        .collect(
            Collectors.groupingBy(
                Map.Entry::getKey,
                Collectors.mapping(Map.Entry::getValue, Collectors.toUnmodifiableList())));
  }

  @Override
  public String name() {
    return "networkIngress";
  }
}
