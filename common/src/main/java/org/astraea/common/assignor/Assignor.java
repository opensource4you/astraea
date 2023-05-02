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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
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
import org.astraea.common.cost.NetworkIngressCost;
import org.astraea.common.metrics.JndiClient;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.collector.MetricStore;
import org.astraea.common.partitioner.PartitionerUtils;

/** Abstract assignor implementation which does some common work (e.g., configuration). */
public abstract class Assignor implements ConsumerPartitionAssignor, Configurable {
  public static final String COST_PREFIX = "assignor.cost";
  public static final String JMX_PORT = "jmx.port";
  Function<Integer, Integer> jmxPortGetter =
      (id) -> {
        throw new NoSuchElementException("must define either broker.x.jmx.port or jmx.port");
      };
  HasPartitionCost costFunction = HasPartitionCost.EMPTY;
  // TODO: need to track the performance when using the assignor in large scale consumers, see
  // https://github.com/skiptests/astraea/pull/1162#discussion_r1036285677
  protected MetricStore metricStore = null;

  protected Admin admin = null;

  /**
   * Perform the group assignment given the member subscriptions and current cluster metadata.
   *
   * @param subscriptions Map from the member id to their respective topic subscription.
   * @param clusterInfo Current cluster information fetched by admin.
   * @return Map from each member to the list of partitions assigned to them.
   */
  protected abstract Map<String, List<TopicPartition>> assign(
      Map<String, SubscriptionInfo> subscriptions, ClusterInfo clusterInfo);

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
        .filter(i -> (metricStore == null || !metricStore.identities().contains(i.id())))
        .collect(Collectors.toMap(NodeInfo::id, NodeInfo::host));
  }

  /**
   * update cluster information
   *
   * @return cluster information
   */
  private ClusterInfo updateClusterInfo() {
    return admin.topicNames(false).thenCompose(admin::clusterInfo).toCompletableFuture().join();
  }

  // -----------------------[kafka method]-----------------------//

  @Override
  public final GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
    var clusterInfo = updateClusterInfo();
    // convert Kafka's data structure to ours
    var subscriptionsPerMember = GroupSubscriptionInfo.from(groupSubscription).groupSubscription();

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
    admin =
        config
            .string(ConsumerConfigs.BOOTSTRAP_SERVERS_CONFIG)
            .map(Admin::of)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        ConsumerConfigs.BOOTSTRAP_SERVERS_CONFIG + " must be defined"));
    var costFunctions =
        Utils.costFunctions(
            config.filteredPrefixConfigs(COST_PREFIX).raw(), HasPartitionCost.class, config);
    var customJMXPort = PartitionerUtils.parseIdJMXPort(config);
    var defaultJMXPort = config.integer(JMX_PORT);
    this.costFunction =
        costFunctions.isEmpty()
            ? HasPartitionCost.of(Map.of(new NetworkIngressCost(config), 1D))
            : HasPartitionCost.of(costFunctions);
    this.jmxPortGetter =
        id ->
            Optional.ofNullable(customJMXPort.get(id))
                .or(() -> defaultJMXPort)
                .orElseThrow(
                    () -> new NoSuchElementException("failed to get jmx port for broker: " + id));
    Supplier<CompletionStage<Map<Integer, MBeanClient>>> clientSupplier =
        () ->
            admin
                .brokers()
                .thenApply(
                    brokers -> {
                      var map = new HashMap<Integer, JndiClient>();
                      brokers.forEach(
                          b ->
                              map.put(
                                  b.id(), JndiClient.of(b.host(), jmxPortGetter.apply(b.id()))));
                      // add local client to fetch consumer metrics
                      map.put(-1, JndiClient.local());
                      return Collections.unmodifiableMap(map);
                    });
    metricStore =
        MetricStore.builder()
            .localReceiver(clientSupplier)
            .sensorsSupplier(
                () ->
                    this.costFunction
                        .metricSensor()
                        .map(s -> Map.of(s, (BiConsumer<Integer, Exception>) (integer, e) -> {}))
                        .orElse(Map.of()))
            .build();
    configure(config);
  }
}
