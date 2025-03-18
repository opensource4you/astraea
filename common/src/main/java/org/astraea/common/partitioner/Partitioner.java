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
import org.apache.kafka.common.Cluster;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.producer.ProducerConfigs;

public abstract class Partitioner implements org.apache.kafka.clients.producer.Partitioner {
  public static final String COST_PREFIX = "partitioner.cost";
  private static final Duration CLUSTER_INFO_LEASE = Duration.ofSeconds(15);

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
    Utils.close(admin);
  }

  // -----------------------[kafka method]-----------------------//

  @Override
  public final void configure(Map<String, ?> configs) {
    var config =
        new Configuration(
            configs.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));
    config.string(ProducerConfigs.BOOTSTRAP_SERVERS_CONFIG).ifPresent(s -> admin = Admin.of(s));
    configure(config);
    tryToUpdate();
  }

  @Override
  public final int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    tryToUpdate();
    final int target;
    if (!clusterInfo.topicNames().contains(topic)) {
      // the cached cluster info is not updated, so we just return a random partition
      var ps = cluster.availablePartitionsForTopic(topic);
      target = ps.isEmpty() ? 0 : ps.get((int) (Math.random() * ps.size())).partition();
    } else target = partition(topic, keyBytes, valueBytes, clusterInfo);
    return target;
  }

  void tryToUpdate() {
    if (admin == null) return;
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
    }
  }
}
