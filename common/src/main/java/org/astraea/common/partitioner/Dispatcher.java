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
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
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

  default void doClose() {}

  /**
   * Use the producer to get the scheduler, allowing you to control it for interdependent
   * messages.Interdependent message will be sent to the same partition. The system will
   * automatically select the node with the best current condition as the target node.
   * Action:Dispatcher states can interfere with each other when multiple producers are in the same
   * thread. Each Thread can only support one producer. For example:
   *
   * <pre>{
   * @Code
   * Dispatch.startInterdependent(producer);
   * producer.send();
   * Dispatch.endInterdependent(producer);
   * }</pre>
   *
   * Begin interdependence function.Let the next messages be interdependent.
   *
   * @param producer Kafka producer
   */
  // TODO One thread supports multiple producers.
  static void beginInterdependent(org.apache.kafka.clients.producer.Producer<?, ?> producer) {
    THREAD_LOCAL.get().isInterdependent = true;
  }

  /**
   * Use the producer to get the scheduler, allowing you to control it for interdependent
   * messages.Interdependent message will be sent to the same partition. The system will
   * automatically select the node with the best current condition as the target node.
   * Action:Dispatcher states can interfere with each other when multiple producers are in the same
   * thread. Each Thread can only support one producer. For example:
   *
   * <pre>{
   * @Code
   * Dispatch.startInterdependent(producer);
   * producer.send();
   * Dispatch.endInterdependent(producer);
   * }</pre>
   *
   * Begin interdependence function.Let the next messages be interdependent.
   *
   * @param producer Kafka producer
   */
  // TODO One thread supports multiple producers.
  static void beginInterdependent(org.astraea.common.producer.Producer<?, ?> producer) {
    THREAD_LOCAL.get().isInterdependent = true;
  }

  /**
   * Close interdependence function.Send data using the original Dispatcher logic.
   *
   * @param producer Kafka producer
   */
  static void endInterdependent(Producer<?, ?> producer) {
    THREAD_LOCAL.remove();
  }
  /**
   * Close interdependence function.Send data using the original Dispatcher logic.
   *
   * @param producer Kafka producer
   */
  static void endInterdependent(org.astraea.common.producer.Producer<?, ?> producer) {
    THREAD_LOCAL.remove();
  }

  /** close this dispatcher. This method is executed only once. */
  @Override
  default void close() {
    doClose();
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
    var interdependent = THREAD_LOCAL.get();
    if (interdependent.isInterdependent && Utils.notNegative(interdependent.targetPartitions))
      return interdependent.targetPartitions;
    var target =
        partition(
            topic,
            keyBytes == null ? new byte[0] : keyBytes,
            valueBytes == null ? new byte[0] : valueBytes,
            CLUSTER_CACHE.computeIfAbsent(cluster, ignored -> ClusterInfo.of(cluster)));
    interdependent.targetPartitions = target;
    return target;
  }

  @Override
  default void onNewBatch(String topic, Cluster cluster, int prevPartition) {}

  class Interdependent {
    boolean isInterdependent = false;
    private int targetPartitions = -1;
  }
}
