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

    return greedyAssign(subscriptions, cost, incompatiblePartition);
  }

  /**
   * Using a greedy strategy to assign partitions to consumers, selecting the consumer with the
   * lowest cost each time to assign.
   *
   * <p>If there are incompatible partitions assigned to the same consumer, perform the reassigning
   * to avoid assigning incompatible partitions to the same consumer.
   *
   * @param subscriptions the subscription of consumers
   * @param costs partition cost
   * @param incompatible incompatible partitions calculated by cost function
   * @return the assignment by greedyAssign
   */
  private Map<String, List<TopicPartition>> greedyAssign(
      Map<String, SubscriptionInfo> subscriptions,
      Map<TopicPartition, Double> costs,
      Map<TopicPartition, Set<TopicPartition>> incompatible) {
    var assignment = Combinator.greedy().combine(subscriptions, costs);
    return Shuffler.incompatible(4000).shuffle(subscriptions, assignment, incompatible, costs);
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
    return "costAware";
  }
}
