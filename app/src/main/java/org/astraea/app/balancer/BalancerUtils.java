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
package org.astraea.app.balancer;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.metrics.HasBeanObject;

public class BalancerUtils {

  /**
   * Update the replicas of ClusterInfo according to the given ClusterLogAllocation. The returned
   * {@link ClusterInfo} will have some of its replicas replaced by the replicas inside the given
   * {@link ClusterLogAllocation}. Since {@link ClusterLogAllocation} might only cover a subset of
   * topic/partition in the associated cluster. Only the replicas related to the covered
   * topic/partition get updated.
   *
   * <p>This method intended to offer a way to describe a cluster with some of its state modified
   * manually.
   *
   * @param clusterInfo to get updated
   * @param allocation offers new host and data folder
   * @return new cluster info
   */
  public static ClusterInfo<Replica> update(
      ClusterInfo<Replica> clusterInfo, ClusterLogAllocation allocation) {
    var newReplicas =
        clusterInfo.replicas().stream()
            .collect(Collectors.groupingBy(r -> TopicPartition.of(r.topic(), r.partition())))
            .entrySet()
            .stream()
            .map(
                entry ->
                    allocation.topicPartitions().contains(entry.getKey())
                        ? allocation.logPlacements(entry.getKey())
                        : entry.getValue())
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableList());

    return ClusterInfo.of(clusterInfo.nodes(), newReplicas);
  }

  public static Thread progressWatch(String title, double totalTasks, Supplier<Double> accTasks) {
    AtomicInteger counter = new AtomicInteger();

    Supplier<String> nextProgressBar =
        () -> {
          int blockCount = 20;
          double percentagePerBlock = 1.0 / blockCount;
          double now = accTasks.get();
          double currentProgress = now / totalTasks;
          int fulfilled = Math.min((int) (currentProgress / percentagePerBlock), blockCount);
          int rollingBlock = blockCount - fulfilled >= 1 ? 1 : 0;
          int emptyBlocks = blockCount - rollingBlock - fulfilled;

          String rollingText = "-\\|/";
          String filled = String.join("", Collections.nCopies(fulfilled, "-"));
          String rolling =
              String.join(
                  "",
                  Collections.nCopies(
                      rollingBlock, "" + rollingText.charAt(counter.getAndIncrement() % 4)));
          String empty = String.join("", Collections.nCopies(emptyBlocks, " "));
          return String.format("[%s%s%s] (%.2f/%.2f)", filled, rolling, empty, now, totalTasks);
        };

    Runnable progressWatch =
        () -> {
          while (!Thread.currentThread().isInterrupted()) {
            System.out.print("[" + title + "] " + nextProgressBar.get() + '\r');
            try {
              TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
              break;
            }
          }
          System.out.println("[" + title + "] " + nextProgressBar.get() + '\r');
          System.out.println();
        };

    return new Thread(progressWatch);
  }

  static double evaluateCost(
      ClusterInfo<Replica> clusterInfo,
      Map<HasClusterCost, Map<Integer, Collection<HasBeanObject>>> metrics) {
    var scores =
        metrics.keySet().stream()
            .map(
                cf -> {
                  var theMetrics = metrics.get(cf);
                  var clusterBean = ClusterBean.of(theMetrics);
                  return Map.entry(cf, cf.clusterCost(clusterInfo, clusterBean).value());
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    return aggregateFunction(scores);
  }

  /** the lower, the better. */
  static double aggregateFunction(Map<HasClusterCost, Double> scores) {
    // use the simple summation result, treat every cost equally.
    return scores.values().stream().mapToDouble(x -> x).sum();
  }
}
