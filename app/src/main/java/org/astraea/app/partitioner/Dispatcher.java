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
package org.astraea.app.partitioner;

import java.security.Key;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.metrics.stats.Value;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.common.Utils;

public interface Dispatcher extends Partitioner {
  /**
   * cache the cluster info to reduce the cost of converting cluster. Producer does not update
   * Cluster frequently, so it is ok to cache it.
   */
  ConcurrentHashMap<Cluster, ClusterInfo> CLUSTER_CACHE = new ConcurrentHashMap<>();

  Interdependent INTERDEPENDENT = new Interdependent();

  //  NodeTopicSizeCost nodeTopicSizeCost = new NodeTopicSizeCost();
  /**
   * Compute the partition for the given record.
   *
   * @param topic The topic name
   * @param key The key to partition on
   * @param value The value to partition
   * @param clusterInfo The current cluster metadata
   */
  int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo);

  /**
   * configure this dispatcher. This method is called only once.
   *
   * @param config configuration
   */
  default void configure(Configuration config) {}

  /**
   * Use the producer to get the scheduler, allowing you to control it for interdependent
   * messages.Interdependent message will be sent to the same partition. The system will
   * automatically select the node with the best current condition as the target node. For example:
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
  static void beginInterdependent(Producer<Key, Value> producer) {
    var dispatcher = dispatcher(producer);
    dispatcher.begin(dispatcher);
  }

  /**
   * Close interdependence function.Send data using the original Dispatcher logic.
   *
   * @param producer Kafka producer
   */
  static void endInterdependent(Producer<Key, Value> producer) {
    var dispatcher = dispatcher(producer);
    dispatcher.end(dispatcher);
  }

  private static Dispatcher dispatcher(Producer<Key, Value> producer) {
    var dispatcher = Utils.reflectionAttribute(producer, "partitioner");
    if (!(dispatcher instanceof Dispatcher))
      throw new RuntimeException(dispatcher.getClass().getName() + "is not Astraea dispatcher.");
    return (Dispatcher) dispatcher;
  }

  /** close this dispatcher. This method is executed only once. */
  @Override
  default void close() {}

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
    return INTERDEPENDENT.isInterdependent
        ? INTERDEPENDENT.interdependentPartition(
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

  private void begin(Dispatcher dispatcher) {
    synchronized (INTERDEPENDENT) {
      INTERDEPENDENT.targetPartitions.putIfAbsent(dispatcher.hashCode(), -1);
      INTERDEPENDENT.isInterdependent = true;
    }
  }

  private void end(Dispatcher dispatcher) {
    synchronized (INTERDEPENDENT) {
      INTERDEPENDENT.isInterdependent = false;
      INTERDEPENDENT.targetPartitions.replace(dispatcher.hashCode(), -1);
    }
  }

  class Interdependent {
    private boolean isInterdependent = false;
    private final Map<Integer, Integer> targetPartitions = new ConcurrentHashMap<>();

    private int interdependentPartition(
        Dispatcher dispatcher,
        String topic,
        byte[] keyBytes,
        byte[] valueBytes,
        ClusterInfo cluster) {
      var targetPartition = targetPartitions.get(dispatcher.hashCode());
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
      if (targetPartitions.get(dispatcher.hashCode()) == -1)
        targetPartitions.replace(dispatcher.hashCode(), targetPartition);
      return targetPartitions.get(dispatcher.hashCode());
    }
  }
}
