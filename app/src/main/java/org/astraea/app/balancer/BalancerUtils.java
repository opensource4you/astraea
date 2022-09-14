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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.management.remote.JMXServiceURL;
import org.astraea.app.balancer.executor.RebalancePlanExecutor;
import org.astraea.app.balancer.generator.RebalancePlanGenerator;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.metrics.IdentifiedFetcher;
import org.astraea.app.balancer.metrics.MetricSource;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.cost.Configuration;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.metrics.HasBeanObject;

public class BalancerUtils {

  /**
   * Update the replicas of ClusterInfo according to ClusterLogAllocation. Noted that only "broker"
   * and "data folder" get updated. The replicas matched to nothing from ClusterLogAllocation won't
   * get any update.
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
            .flatMap(
                entry -> {
                  var lps = allocation.logPlacements(entry.getKey());
                  var replicas = entry.getValue();
                  return IntStream.range(0, replicas.size())
                      .mapToObj(
                          index -> {
                            var previous = replicas.get(index);
                            // return previous replica due to no new information
                            if (index >= lps.size()) return previous;
                            var lp = lps.get(index);
                            return Replica.of(
                                previous.topic(),
                                previous.partition(),
                                clusterInfo.node(lp.broker()),
                                previous.lag(),
                                previous.size(),
                                index == 0,
                                previous.inSync(),
                                previous.isFuture(),
                                previous.isOffline(),
                                previous.isPreferredLeader(),
                                lp.dataFolder());
                          });
                })
            .collect(Collectors.toUnmodifiableList());

    return ClusterInfo.of(clusterInfo.nodes(), newReplicas);
  }

  /**
   * Create a {@link ClusterInfo} with its log placement replaced by {@link ClusterLogAllocation}.
   * Every log will be marked as online & synced. Based on the given content in {@link
   * ClusterLogAllocation}, some logs might not have its data directory specified. Noted that this
   * method doesn't check if the given logs is suitable & exists in the cluster info base. the beans
   * alongside the based cluster info might be out-of-date or even completely meaningless.
   *
   * @param clusterInfo the based cluster info
   * @param allocation the log allocation to replace {@link ClusterInfo}'s log placement. If the
   *     allocation implementation is {@link ClusterLogAllocation} then the given instance will be
   *     locked.
   * @return a {@link ClusterInfo} with its log placement replaced.
   */
  public static ClusterInfo<Replica> merge(
      ClusterInfo<? extends Replica> clusterInfo, ClusterLogAllocation allocation) {
    return new ClusterInfo<>() {
      // TODO: maybe add a field to tell if this cluster info is mocked.
      private final Map<Integer, NodeInfo> nodeIdMap =
          nodes().stream().collect(Collectors.toUnmodifiableMap(NodeInfo::id, Function.identity()));
      private final List<Replica> replicas =
          allocation.topicPartitions().stream()
              .map(tp -> Map.entry(tp, allocation.logPlacements(tp)))
              .flatMap(
                  entry -> {
                    var tp = entry.getKey();
                    var logs = entry.getValue();

                    return IntStream.range(0, logs.size())
                        .mapToObj(
                            i ->
                                // TODO: too many fake data!!! we should use another data structure
                                // https://github.com/skiptests/astraea/issues/526
                                Replica.of(
                                    tp.topic(),
                                    tp.partition(),
                                    nodeIdMap.get(logs.get(i).broker()),
                                    0,
                                    -1,
                                    i == 0,
                                    true,
                                    false,
                                    false,
                                    false,
                                    logs.get(i).dataFolder()));
                  })
              .collect(Collectors.toUnmodifiableList());

      @Override
      public List<NodeInfo> nodes() {
        return clusterInfo.nodes();
      }

      @Override
      public Stream<Replica> replicaStream() {
        return replicas.stream();
      }
    };
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

  public static <T extends RebalancePlanGenerator> T constructGenerator(
      Class<T> generatorClass, Configuration configuration) {
    try {
      // case 0: create cost function by configuration
      var constructor = generatorClass.getConstructor(Configuration.class);
      return Utils.packException(() -> constructor.newInstance(configuration));
    } catch (NoSuchMethodException e) {
      // case 1: create cost function by empty constructor
      return Utils.packException(() -> generatorClass.getConstructor().newInstance());
    }
  }

  public static <T extends RebalancePlanExecutor> T constructExecutor(
      Class<T> executorClass, Configuration configuration) {
    try {
      // case 0: create cost function by configuration
      var constructor = executorClass.getConstructor(Configuration.class);
      return Utils.packException(() -> constructor.newInstance(configuration));
    } catch (NoSuchMethodException e) {
      // case 1: create cost function by empty constructor
      return Utils.packException(() -> executorClass.getConstructor().newInstance());
    }
  }

  public static <T extends MetricSource> T constructMetricSource(
      Class<T> metricClass,
      Map<Integer, JMXServiceURL> serviceURLMap,
      Collection<IdentifiedFetcher> fetchers,
      Configuration configuration) {
    try {
      // case 0: create cost function by configuration
      var constructor =
          metricClass.getConstructor(Map.class, Collection.class, Configuration.class);
      return Utils.packException(
          () -> constructor.newInstance(serviceURLMap, fetchers, configuration));
    } catch (NoSuchMethodException e) {
      // case 1: create cost function by empty constructor
      return Utils.packException(() -> metricClass.getConstructor().newInstance());
    }
  }
}
