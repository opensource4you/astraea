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

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.cost.HasPartitionCost;
import org.astraea.common.cost.ReplicaLeaderSizeCost;
import org.astraea.common.metrics.collector.LocalMetricCollector;
import org.astraea.common.metrics.collector.MetricCollector;
import org.astraea.common.partitioner.PartitionerUtils;

/** Abstract assignor implementation which does some common work (e.g., configuration). */
public abstract class Assignor implements ConsumerPartitionAssignor, Configurable {
  public static final String COST_PREFIX = "assignor.cost";
  public static final String JMX_PORT = "jmx.port";
  Function<Integer, Optional<Integer>> jmxPortGetter = (id) -> Optional.empty();
  private String bootstrap;
  HasPartitionCost costFunction = HasPartitionCost.EMPTY;
  // TODO: metric collector may be configured by user in the future.
  // TODO: need to track the performance when using the assignor in large scale consumers, see
  // https://github.com/skiptests/astraea/pull/1162#discussion_r1036285677
  protected MetricCollector metricCollector = null;

  /**
   * Perform the group assignment given the member subscriptions and current cluster metadata.
   *
   * @param subscriptions Map from the member id to their respective topic subscription.
   * @param clusterInfo Current cluster information fetched by admin.
   * @return Map from each member to the list of partitions assigned to them.
   */
  protected abstract Map<String, List<TopicPartition>> assign(
      Map<String, org.astraea.common.assignor.Subscription> subscriptions, ClusterInfo clusterInfo);
  // TODO: replace the topicPartitions by ClusterInfo after Assignor is able to handle Admin
  // https://github.com/skiptests/astraea/issues/1409

  /**
   * Parse config to get JMX port and cost function type.
   *
   * @param config configuration
   */
  protected void configure(Configuration config) {}

  // -----------------------[helper]-----------------------//

  /**
   * check the nodes which wasn't register yet.
   *
   * @param nodes List of node information
   * @return Map from each broker id to broker host
   */
  protected Map<Integer, String> checkUnregister(List<NodeInfo> nodes) {
    return nodes.stream()
        .filter(
            i -> (metricCollector == null || !metricCollector.listIdentities().contains(i.id())))
        .collect(Collectors.toMap(NodeInfo::id, NodeInfo::host));
  }

  /**
   * register the JMX for metric collector. only register the JMX that is not registered yet.
   *
   * @param unregister Map from each broker id to broker host
   */
  protected void registerJMX(Map<Integer, String> unregister) {
    if (metricCollector instanceof LocalMetricCollector) {
      var localCollector = (LocalMetricCollector) metricCollector;
      unregister.forEach(
          (id, host) ->
              localCollector.registerJmx(
                  id, InetSocketAddress.createUnresolved(host, jmxPortGetter.apply(id).get())));
    }
  }

  /**
   * update cluster information
   *
   * @return cluster information
   */
  private ClusterInfo updateClusterInfo() {
    try (Admin admin = Admin.of(bootstrap)) {
      return admin.topicNames(false).thenCompose(admin::clusterInfo).toCompletableFuture().join();
    }
  }

  // -----------------------[kafka method]-----------------------//

  @Override
  public final GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
    var clusterInfo = updateClusterInfo();
    // convert Kafka's data structure to ours
    var subscriptionsPerMember =
        org.astraea.common.assignor.GroupSubscription.from(groupSubscription).groupSubscription();

    // TODO: Detected if consumers subscribed to the same topics.
    // For now, assume that the consumers only subscribed to identical topics

    return new GroupAssignment(
        assign(subscriptionsPerMember, clusterInfo).entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        new Assignment(
                            e.getValue().stream()
                                .map(TopicPartition::to)
                                .collect(Collectors.toUnmodifiableList())))));
  }

  @Override
  public final void configure(Map<String, ?> configs) {
    var config =
        Configuration.of(
            configs.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));
    config.string(ConsumerConfigs.BOOTSTRAP_SERVERS_CONFIG).ifPresent(s -> bootstrap = s);
    var costFunctions =
        Utils.costFunctions(
            config.filteredPrefixConfigs(COST_PREFIX).raw(), HasPartitionCost.class, config);
    var customJMXPort = PartitionerUtils.parseIdJMXPort(config);
    var defaultJMXPort = config.integer(JMX_PORT);
    this.costFunction =
        costFunctions.isEmpty()
            ? HasPartitionCost.of(Map.of(new ReplicaLeaderSizeCost(), 1D))
            : HasPartitionCost.of(costFunctions);
    this.jmxPortGetter = id -> Optional.ofNullable(customJMXPort.get(id)).or(() -> defaultJMXPort);
    metricCollector =
        MetricCollector.local()
            .interval(Duration.ofSeconds(1))
            .expiration(Duration.ofSeconds(15))
            .addMetricSensors(this.costFunction.metricSensor().stream().collect(Collectors.toSet()))
            .build();
    configure(config);
  }
}
