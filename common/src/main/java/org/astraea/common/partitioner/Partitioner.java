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

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Cluster;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.producer.ProducerConfigs;

public abstract class Partitioner implements org.apache.kafka.clients.producer.Partitioner {
  public static final String COST_PREFIX = "partitioner.cost";
  private static final Duration CLUSTER_INFO_LEASE = Duration.ofSeconds(15);

  static final ThreadLocal<Interdependent> THREAD_LOCAL =
      ThreadLocal.withInitial(Interdependent::new);

  private final AtomicLong lastUpdated = new AtomicLong(-1);
  volatile ClusterInfo clusterInfo = ClusterInfo.empty();
  Admin admin = null;

  /**
   * Compute the partition for the given record.
   *
   * @param topic The topic name
   * @param key The key to partition on
   * @param value The value to partition
   * @param clusterInfo The current cluster metadata
   */
  protected abstract int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo);

  /**
   * configure this partitioner. This method is called only once.
   *
   * @param config configuration
   */
  protected void configure(Configuration config) {}

  protected void onNewBatch(String topic, int prevPartition, ClusterInfo clusterInfo) {}

  @Override
  public void close() {
    if (admin != null) Utils.packException(admin::close);
  }

  // -----------------------[interdependent]-----------------------//

  /**
   * Use the producer to get the scheduler, allowing you to control it for interdependent
   * messages.Interdependent message will be sent to the same partition. The system will
   * automatically select the node with the best current condition as the target node.
   * Action:Partitioner states can interfere with each other when multiple producers are in the same
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
  public static void beginInterdependent(
      org.apache.kafka.clients.producer.Producer<?, ?> producer) {
    THREAD_LOCAL.get().isInterdependent = true;
  }

  /**
   * Use the producer to get the scheduler, allowing you to control it for interdependent
   * messages.Interdependent message will be sent to the same partition. The system will
   * automatically select the node with the best current condition as the target node.
   * Action:Partitioner states can interfere with each other when multiple producers are in the same
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
   * @param producer Astraea producer
   */
  // TODO One thread supports multiple producers.
  // TODO: https://github.com/skiptests/astraea/pull/721#discussion_r973677891
  public static void beginInterdependent(org.astraea.common.producer.Producer<?, ?> producer) {
    beginInterdependent((Producer<?, ?>) Utils.member(producer, "kafkaProducer"));
  }

  /**
   * Close interdependence function.Send data using the original partitioner logic.
   *
   * @param producer Kafka producer
   */
  public static void endInterdependent(org.apache.kafka.clients.producer.Producer<?, ?> producer) {
    THREAD_LOCAL.remove();
  }
  /**
   * Close interdependence function.Send data using the original partitioner logic.
   *
   * @param producer Kafka producer
   */
  // TODO: https://github.com/skiptests/astraea/pull/721#discussion_r973677891
  public static void endInterdependent(org.astraea.common.producer.Producer<?, ?> producer) {
    endInterdependent((Producer<?, ?>) Utils.member(producer, "kafkaProducer"));
  }

  private static class Interdependent {
    boolean isInterdependent = false;
    private int targetPartitions = -1;
  }

  // -----------------------[kafka method]-----------------------//

  @Override
  public final void configure(Map<String, ?> configs) {
    var config =
        Configuration.of(
            configs.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));
    config.string(ProducerConfigs.BOOTSTRAP_SERVERS_CONFIG).ifPresent(s -> admin = Admin.of(s));
    configure(config);
    tryToUpdate();
  }

  @Override
  public final int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    var interdependent = THREAD_LOCAL.get();
    if (interdependent.isInterdependent && interdependent.targetPartitions >= 0)
      return interdependent.targetPartitions;
    tryToUpdate();
    final int target;
    if (!clusterInfo.topicNames().contains(topic)) {
      // the cached cluster info is not updated, so we just return a random partition
      var ps = cluster.availablePartitionsForTopic(topic);
      target = ps.isEmpty() ? 0 : ps.get((int) (Math.random() * ps.size())).partition();
    } else target = partition(topic, keyBytes, valueBytes, clusterInfo);
    interdependent.targetPartitions = target;
    return target;
  }

  boolean tryToUpdate() {
    if (admin == null) return false;
    var now = System.nanoTime();
    // need to refresh cluster info if lease expires
    if (lastUpdated.updateAndGet(last -> now - last >= CLUSTER_INFO_LEASE.toNanos() ? now : last)
        == now) {
      admin
          .topicNames(true)
          .thenCompose(names -> admin.clusterInfo(names))
          .whenComplete(
              (c, e) -> {
                if (c != null) {
                  this.clusterInfo = c;
                  lastUpdated.set(System.nanoTime());
                }
              });
      return true;
    }
    return false;
  }

  @Override
  public final void onNewBatch(String topic, Cluster cluster, int prevPartition) {
    onNewBatch(topic, prevPartition, clusterInfo);
  }
}
