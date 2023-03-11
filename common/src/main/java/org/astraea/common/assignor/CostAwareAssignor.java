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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
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

/**
 * This assignor scores the partitions by cost function(s) that user given. Each cost function
 * evaluate the partitions' cost in each node by metrics depend on which cost function user use. The
 * default cost function ranks partitions that are in the same node by NetworkIngressCost{@link
 * org.astraea.common.cost.NetworkIngressCost}
 *
 * <p>When get the partitions' cost of each node, assignor would assign partitions to consumers base
 * on node. Each consumer would get the partitions with "the similar cost" from same node.
 *
 * <p>The important configs are JMX port, MAX_WAIT_BEAN, MAX_TRAFFIC_MiB_INTERVAL. Most cost
 * function need the JMX metrics to score partitions. Normally, all brokers use the same JMX port,
 * so you could just define the `jmx.port=12345`. If one of brokers uses different JMX client port,
 * you can define `broker.1001.jmx.port=3456` (`1001` is the broker id) to replace the value of
 * `jmx.port`. If the jmx port is undefined, only local mbean client is created for each cost
 * function.
 *
 * <p>MAX_WAIT_BEAN is the config of setting the amount of time waiting for fetch ClusterBean.
 * MAX_TRAFFIC_MiB_INTERVAL is the config of setting how traffic similar is. You can define these
 * config by `max.wait.bean=10` or `max.traffic.mib.interval=15`
 */
public class CostAwareAssignor extends Assignor {

  @Override
  protected Map<String, List<TopicPartition>> assign(
      Map<String, org.astraea.common.assignor.Subscription> subscriptions,
      ClusterInfo clusterInfo) {
    registerUnregisterNode(clusterInfo);
    var subscribedTopics =
        subscriptions.values().stream()
            .map(org.astraea.common.assignor.Subscription::topics)
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableSet());

    // wait for clusterBean
    Utils.waitFor(
        () ->
            !metricCollector.clusterBean().all().isEmpty()
                && metricCollector.clusterBean().topics().containsAll(subscribedTopics),
        Duration.ofSeconds(maxWaitBean));
    var clusterBean = metricCollector.clusterBean();

    var partitionCost = costFunction.partitionCost(clusterInfo, clusterBean).value();
    var costPerBroker = wrapCostBaseOnNode(clusterInfo, subscribedTopics, partitionCost);
    var intervalPerBroker = estimateIntervalTraffic(clusterInfo, clusterBean, costPerBroker);
    return greedyAssign(costPerBroker, subscriptions, intervalPerBroker);
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
   * perform assign algorithm ensure that similar loads within a node would be assigned to the same
   * consumer.
   *
   * @param costs the tp and their cost within a node
   * @param subscription All subscription for consumers
   * @param intervalPerBroker Transforming the traffic of each node into cost
   * @return the final assignment
   */
  Map<String, List<TopicPartition>> greedyAssign(
      Map<Integer, Map<TopicPartition, Double>> costs,
      Map<String, org.astraea.common.assignor.Subscription> subscription,
      Map<Integer, Double> intervalPerBroker) {
    // TODO: need detect consumer with different subscription
    // initial
    var assignment = new HashMap<String, List<TopicPartition>>();
    var consumers = subscription.keySet();
    for (var consumer : consumers) {
      assignment.put(consumer, new ArrayList<>());
    }
    var costPerConsumer =
        assignment.keySet().stream()
            .map(c -> Map.entry(c, (double) 0))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    costs.forEach(
        (brokerId, cost) ->
            assignPerNode(cost, costPerConsumer, intervalPerBroker.get(brokerId))
                .forEach((consumer, result) -> assignment.get(consumer).addAll(result)));
    return assignment;
  }

  /**
   * aggregate the assignment of with interval and without interval
   *
   * @param partitionCost partition cost
   * @param consumerCost the consumer with its total cost
   * @param interval the config of `max.traffic.mib.interval`
   * @return the assignment of a node
   */
  Map<String, List<TopicPartition>> assignPerNode(
      Map<TopicPartition, Double> partitionCost,
      Map<String, Double> consumerCost,
      Double interval) {
    // TODO: avoid numberOfConsumer < intervalAssignment.size()
    var intervalAssignment = groupPartitionWithInterval(partitionCost, interval);
    var groupNumberOfNonInterval = consumerCost.size() - intervalAssignment.size();
    Map<Double, HashMap<TopicPartition, Double>> result;

    if (groupNumberOfNonInterval == 0) {
      var upperBound = interval * (maxUpperBoundMiB / maxTrafficMiBInterval);
      var dontCareSimilarCost =
          partitionCost.entrySet().stream()
              .filter(e -> e.getValue() >= upperBound)
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      intervalAssignment.get(upperBound).putAll(dontCareSimilarCost);
      result = intervalAssignment;
    } else {
      var dontCareSimilar =
          groupPartitionWithoutInterval(partitionCost, interval, groupNumberOfNonInterval);
      result =
          Stream.concat(intervalAssignment.entrySet().stream(), dontCareSimilar.entrySet().stream())
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    var assignOrder =
        result.entrySet().stream()
            .map(
                e -> {
                  var id = e.getKey();
                  var total = e.getValue().values().stream().mapToDouble(x -> x).sum();
                  return Map.entry(id, total);
                })
            .sorted(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    Collections.reverse(assignOrder);

    var tmpConsumerCost = new HashMap<>(consumerCost);
    Supplier<String> lowestCostConsumer =
        () -> Collections.min(tmpConsumerCost.entrySet(), Map.Entry.comparingByValue()).getKey();

    return assignOrder.stream()
        .map(
            id -> {
              var consumer = lowestCostConsumer.get();

              tmpConsumerCost.remove(consumer);
              consumerCost.compute(
                  consumer,
                  (ignore, cost) ->
                      cost + result.get(id).values().stream().mapToDouble(x -> x).sum());
              return Map.entry(
                  consumer,
                  result.get(id).keySet().stream().collect(Collectors.toUnmodifiableList()));
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Assign the partitions which cost is larger than `max.upper.bound.mib`. For costs that exceed
   * the upper bound, they should be evenly distributed as much as possible.
   *
   * @param partitionCost partition cost based on the cost function user used
   * @param interval the config of `max.traffic.mib.interval`
   * @param groupNumber the group number must equal to the (number of consumers - number of group
   *     within interval)
   * @return the part of assignment
   */
  protected Map<Double, HashMap<TopicPartition, Double>> groupPartitionWithoutInterval(
      Map<TopicPartition, Double> partitionCost, Double interval, int groupNumber) {
    var upperBound = interval * (maxUpperBoundMiB / maxTrafficMiBInterval);
    var dontCareSimilarCost =
        partitionCost.entrySet().stream()
            .filter(e -> e.getValue() >= upperBound)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    if (groupNumber == 1) return Map.of(1.0, new HashMap<>(dontCareSimilarCost));

    var result =
        IntStream.range(1, groupNumber + 1)
            .mapToObj(i -> Map.entry((double) i, new HashMap<TopicPartition, Double>()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var tmpCost = result.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> 0.0));
    Supplier<Double> minCost =
        () -> Collections.min(tmpCost.entrySet(), Map.Entry.comparingByValue()).getKey();
    dontCareSimilarCost.forEach(
        (tp, cost) -> {
          var min = minCost.get();
          result.get(min).put(tp, cost);
          tmpCost.computeIfPresent(min, (ignore, costValue) -> costValue + cost);
        });
    return result.entrySet().stream()
        .filter(e -> !e.getValue().isEmpty())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Assign the partitions which cost is less than `max.upper.bound.mib`. The reason for needing to
   * look at the upper bound is to assign partitions with similar amounts of traffic to the same
   * consumer.
   *
   * @param partitionCost the partition cost
   * @param interval the config of `max.traffic.mib.interval`, Distinguishing the interval of
   *     traffic
   * @return the part of assignment
   */
  protected Map<Double, HashMap<TopicPartition, Double>> groupPartitionWithInterval(
      Map<TopicPartition, Double> partitionCost, Double interval) {
    // upper = 50, interval = 10
    // range: 0~10, 10~20, 20~30, 30~40, 40~50
    // upper = 35, interval = 10
    // range: 0~10, 10~20, 20~30, 30~35
    var upperBoundCost = interval * (maxUpperBoundMiB / maxTrafficMiBInterval);
    var groupNumbers = (int) Math.ceil(maxUpperBoundMiB / maxTrafficMiBInterval);
    var intervals =
        IntStream.range(1, groupNumbers + 1)
            .mapToObj(
                i ->
                    Map.entry(
                        Math.min(interval * i, upperBoundCost),
                        new HashMap<TopicPartition, Double>()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var orderedList = intervals.keySet().stream().sorted().collect(Collectors.toUnmodifiableList());
    // Aggregate similar traffic to the same key of intervals.
    // If cost is larger than upperBound cost, put it into dontCareSimilarCost
    partitionCost.entrySet().stream()
        .filter(e -> e.getValue() < upperBoundCost)
        .forEach(
            e -> {
              for (var i : orderedList) {
                if (e.getValue() < i) {
                  intervals.get(i).put(e.getKey(), e.getValue());
                  break;
                }
              }
            });
    return intervals.entrySet().stream()
        .filter(e -> !e.getValue().isEmpty())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
  /**
   * Wrap the partition and cost based on nodes. This method is used to process special cost, e.g.,
   * `LogSizeCost` and `NetworkIngressCost`
   *
   * @param clusterInfo the cluster information that admin fetch
   * @param topics total topics that consumers subscribed
   * @param cost partition cost calculated by cost function
   * @return Map from each broker id to partitions' cost
   */
  Map<Integer, Map<TopicPartition, Double>> wrapCostBaseOnNode(
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
   * For all nodes, estimate the cost of given traffic. The assignor would use the interval cost to
   * assign the partition with similar cost to the same consumer.
   *
   * @param clusterInfo the clusterInfo
   * @param clusterBean the clusterBean
   * @param tpCostPerBroker the partition cost of every broker
   * @return Map from broker id to the cost of given traffic
   */
  Map<Integer, Double> estimateIntervalTraffic(
      ClusterInfo clusterInfo,
      ClusterBean clusterBean,
      Map<Integer, Map<TopicPartition, Double>> tpCostPerBroker) {
    var interval = DataRate.MiB.of((long) maxTrafficMiBInterval).perSecond().byteRate();
    // get partitions' cost
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
              // select a partition with its network ingress cost
              var tpCost =
                  e.getValue().entrySet().stream()
                      .filter(entry -> entry.getValue() > 0.0)
                      .findFirst()
                      .orElseThrow();
              var traffic = partitionsTraffic.get(tpCost.getKey());
              var normalizedCost = tpCost.getValue();
              // convert the interval value to cost
              var result = normalizedCost / (traffic / interval);
              return Map.entry(e.getKey(), result);
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * the helper method that estimate the interval traffic
   *
   * @param clusterInfo cluster info
   * @return Map from BrokerTopic to Replica
   */
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
