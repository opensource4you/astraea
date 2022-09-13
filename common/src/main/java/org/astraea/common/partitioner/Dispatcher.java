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
package org.astraea.common.partitioner;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.cost.Configuration;

public interface Dispatcher extends Partitioner {
  /**
   * cache the cluster info to reduce the cost of converting cluster. Producer does not update
   * Cluster frequently, so it is ok to cache it.
   */
  ConcurrentHashMap<Cluster, ClusterInfo<ReplicaInfo>> CLUSTER_CACHE = new ConcurrentHashMap<>();

  ThreadLocal<Interdependent> THREAD_LOCAL = ThreadLocal.withInitial(Interdependent::new);

  /**
   * Compute the partition for the given record.
   *
   * @param topic The topic name
   * @param key The key to partition on
   * @param value The value to partition
   * @param clusterInfo The current cluster metadata
   */
  int partition(String topic, byte[] key, byte[] value, ClusterInfo<ReplicaInfo> clusterInfo);

  /**
   * configure this dispatcher. This method is called only once.
   *
   * @param config configuration
   */
  default void configure(Configuration config) {}

  default void closeDispatcher() {}

  /**
   * Use the producer to get the scheduler, allowing you to control it for interdependent
   * messages.Interdependent message will be sent to the same partition. The system will
   * automatically select the node with the best current condition as the target node.
   * Action:Dispatcher states can interfere with each other when multiple producers are in the same
   * thread. Each Thread can only support one producer.For example:
   *
   * <pre>{
   * @Code
   * Dispatch.startInterdependent();
   * producer.send();
   * Dispatch.endInterdependent();
   * }</pre>
   *
   * Begin interdependence function.Let the next messages be interdependent.
   */
  static void beginInterdependent() {
    THREAD_LOCAL.get().isInterdependent.set(true);
  }

  /** Close interdependence function.Send data using the original Dispatcher logic. */
  static void endInterdependent() {
    THREAD_LOCAL.get().isInterdependent.set(false);
    THREAD_LOCAL.get().targetPartitions.set(-1);
  }

  /** close this dispatcher. This method is executed only once. */
  @Override
  default void close() {
    THREAD_LOCAL.remove();
    closeDispatcher();
  }

  @Override
  default void configure(Map<String, ?> configs) {
    configure(
        Configuration.of(
            configs.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()))));
  }

  @Override
  default int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    return THREAD_LOCAL.get().isInterdependent.get()
        ? THREAD_LOCAL
            .get()
            .interdependentPartition(
                this,
                topic,
                keyBytes,
                valueBytes,
                CLUSTER_CACHE.computeIfAbsent(cluster, ignored -> ClusterInfo.of(cluster)))
        : partition(
            topic,
            keyBytes == null ? new byte[0] : keyBytes,
            valueBytes == null ? new byte[0] : valueBytes,
            CLUSTER_CACHE.computeIfAbsent(cluster, ignored -> ClusterInfo.of(cluster)));
  }

  @Override
  default void onNewBatch(String topic, Cluster cluster, int prevPartition) {}

  class Interdependent {
    private final AtomicBoolean isInterdependent = new AtomicBoolean(false);
    private final AtomicInteger targetPartitions = new AtomicInteger(-1);

    private int interdependentPartition(
        Dispatcher dispatcher,
        String topic,
        byte[] keyBytes,
        byte[] valueBytes,
        ClusterInfo cluster) {
      var targetPartition = targetPartitions.get();
      return Utils.isPositive(targetPartition)
          ? targetPartition
          : targetPartition(
              dispatcher,
              topic,
              keyBytes == null ? new byte[0] : keyBytes,
              valueBytes == null ? new byte[0] : valueBytes,
              cluster);
    }

    private int targetPartition(
        Dispatcher dispatcher,
        String topic,
        byte[] keyBytes,
        byte[] valueBytes,
        ClusterInfo cluster) {
      var targetPartition =
          dispatcher.partition(
              topic,
              keyBytes == null ? new byte[0] : keyBytes,
              valueBytes == null ? new byte[0] : valueBytes,
              cluster);
      if (targetPartitions.get() == -1) targetPartitions.set(targetPartition);
      return targetPartitions.get();
    }
  }
}
