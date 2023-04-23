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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.cost.NoSufficientMetricsException;

/**
 * This assignor scores the partitions by cost function(s) that user given. Each cost function
 * evaluate the partitions' cost in each node by metrics depend on which cost function user use. The
 * default cost function ranks partitions that are in the same node by NetworkIngressCost{@link
 * org.astraea.common.cost.NetworkIngressCost}
 *
 * <p>The important configs are JMX port. Most cost function need the JMX metrics to score
 * partitions. Normally, all brokers use the same JMX port, so you could just define the
 * `jmx.port=12345`. If one of brokers uses different JMX client port, you can define
 * `broker.1001.jmx.port=3456` (`1001` is the broker id) to replace the value of `jmx.port`. If the
 * jmx port is undefined, only local mbean client is created for each cost function.
 */
public class CostAwareAssignor extends Assignor {
  protected static final String MAX_RETRY_TIME = "max.retry.time";
  Duration maxRetryTime = Duration.ofSeconds(30);

  @Override
  protected Map<String, List<TopicPartition>> assign(
      Map<String, SubscriptionInfo> subscriptions, ClusterInfo clusterInfo) {
    var subscribedTopics =
        subscriptions.values().stream()
            .map(SubscriptionInfo::topics)
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableSet());

    // wait for clusterBean
    retry(clusterInfo);

    var clusterBean = metricStore.clusterBean();
    var partitionCost = costFunction.partitionCost(clusterInfo, clusterBean);
    var cost =
        partitionCost.value().entrySet().stream()
            .filter(e -> subscribedTopics.contains(e.getKey().topic()))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    var incompatiblePartition = partitionCost.incompatibility();
    var greedyAssignment = greedyAssign(subscriptions, cost);
    var adjustedAssignment =
        checkIncompatibility(subscriptions, greedyAssignment, incompatiblePartition, cost);

    return adjustedAssignment;
  }

  /**
   * Using a greedy strategy to assign partitions to consumers, selecting the consumer with the
   * lowest cost each time to assign.
   *
   * @param subscriptions the subscription of consumers
   * @param costs partition cost
   * @return the assignment by greedyAssign
   */
  protected Map<String, List<TopicPartition>> greedyAssign(
      Map<String, SubscriptionInfo> subscriptions, Map<TopicPartition, Double> costs) {
    var tmpConsumerCost =
        subscriptions.keySet().stream()
            .collect(Collectors.toMap(Function.identity(), ignore -> 0.0D));

    var lightWeightConsumer =
        (Function<TopicPartition, String>)
            (tp) ->
                tmpConsumerCost.entrySet().stream()
                    .filter(e -> subscriptions.get(e.getKey()).topics().contains(tp.topic()))
                    .min(Map.Entry.comparingByValue())
                    .get()
                    .getKey();

    return costs.entrySet().stream()
        .map(
            e -> {
              var consumer = lightWeightConsumer.apply(e.getKey());
              tmpConsumerCost.compute(consumer, (ignore, totalCost) -> totalCost + e.getValue());
              return Map.entry(consumer, e.getKey());
            })
        .collect(
            Collectors.groupingBy(
                Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
  }

  /**
   * Try to avoid putting incompatible partitions on the same consumer.
   *
   * @param subscriptions the subscription of consumers
   * @param assignment assignment
   * @param incompatible incompatible partition calculated by cost function
   * @param costs partition cost
   * @return assignment that filter out most incompatible partitions
   */
  protected Map<String, List<TopicPartition>> checkIncompatibility(
      Map<String, SubscriptionInfo> subscriptions,
      Map<String, List<TopicPartition>> assignment,
      Map<TopicPartition, Set<TopicPartition>> incompatible,
      Map<TopicPartition, Double> costs) {
    // if there is no incompatible, just return the assignment
    if (incompatible.isEmpty()) return assignment;
    // check the assignment if there are incompatible partitions were put together
    var unsuitable =
        assignment.entrySet().stream()
            .map(
                e ->
                    Map.entry(
                        e.getKey(),
                        e.getValue().stream()
                            .flatMap(tp -> incompatible.get(tp).stream())
                            .collect(Collectors.toUnmodifiableSet())))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    // if there is no incompatible partition put together, just return the assignment
    if (unsuitable.values().stream().allMatch(Set::isEmpty)) return assignment;

    // filter incompatible partitions from assignment to get remaining assignment
    var remaining =
        assignment.keySet().stream()
            .map(
                consumer ->
                    Map.entry(
                        consumer,
                        assignment.get(consumer).stream()
                            .filter(tp -> !unsuitable.get(consumer).contains(tp))
                            .collect(Collectors.toList())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    // calculate remaining cost for further assign
    var remainingCost =
        remaining.entrySet().stream()
            .map(e -> Map.entry(e.getKey(), e.getValue().stream().mapToDouble(costs::get).sum()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    var assigned =
        remaining.values().stream().flatMap(List::stream).collect(Collectors.toUnmodifiableSet());
    var unassigned =
        assignment.values().stream()
            .flatMap(Collection::stream)
            .filter(tp -> !assigned.contains(tp))
            .collect(Collectors.toSet());

    for (var tp : unassigned) {
      String minConsumer;
      // find the consumers that subscribe the topic which we assign now
      var subscribedConsumer =
          subscriptions.entrySet().stream()
              .filter(e -> e.getValue().topics().contains(tp.topic()))
              .map(Map.Entry::getKey)
              .collect(Collectors.toSet());
      // find the consumers that are suitable with the tp
      var suitableConsumer =
          remaining.entrySet().stream()
              .filter(e -> subscribedConsumer.contains(e.getKey()))
              .map(
                  e ->
                      Map.entry(
                          e.getKey(),
                          e.getValue().stream()
                              .flatMap(p -> incompatible.get(p).stream())
                              .collect(Collectors.toSet())))
              .filter(e -> !e.getValue().contains(tp))
              .map(Map.Entry::getKey)
              .collect(Collectors.toSet());

      // if there is no suitable consumer, choose the lowest cost consumer that subscribed topic and
      // assign the tp to it
      // Otherwise, choose the lowest cost consumer from suitable consumers and assign the tp to it
      minConsumer =
          suitableConsumer.isEmpty()
              ? remainingCost.entrySet().stream()
                  .filter(e -> subscribedConsumer.contains(e.getKey()))
                  .min(Map.Entry.comparingByValue())
                  .get()
                  .getKey()
              : remainingCost.entrySet().stream()
                  .filter(e -> suitableConsumer.contains(e.getKey()))
                  .min(Map.Entry.comparingByValue())
                  .get()
                  .getKey();

      remaining.get(minConsumer).add(tp);
      remainingCost.compute(minConsumer, (ignore, totalCost) -> totalCost + costs.get(tp));
    }

    return remaining;
  }

  private void retry(ClusterInfo clusterInfo) {
    var timeoutMs = System.currentTimeMillis() + maxRetryTime.toMillis();
    while (System.currentTimeMillis() < timeoutMs) {
      try {
        var clusterBean = metricStore.clusterBean();
        var partitionCost = costFunction.partitionCost(clusterInfo, clusterBean);
        if (partitionCost.value().values().stream().noneMatch(v -> Double.isNaN(v))) return;
      } catch (NoSufficientMetricsException e) {
        e.printStackTrace();
        Utils.sleep(Duration.ofSeconds(1));
      }
    }
    throw new RuntimeException("Failed to fetch clusterBean due to timeout");
  }

  @Override
  protected void configure(Configuration config) {
    config.duration(MAX_RETRY_TIME).ifPresent(v -> this.maxRetryTime = v);
  }

  @Override
  public String name() {
    return "networkIngress";
  }
}
