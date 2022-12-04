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
package org.astraea.common.consumer.assignor;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.astraea.common.Configuration;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.collector.MetricCollector;
import org.astraea.common.partitioner.PartitionerUtils;

/** Abstract assignor implementation which does some common work (e.g., configuration). */
public abstract class AbstractConsumerPartitionAssignor implements ConsumerPartitionAssignor {
  public static final String JMX_PORT = "jmx.port";
  Function<Integer, Optional<Integer>> jmxPortGetter = (id) -> Optional.empty();
  private boolean register = false;
  // TODO: metric collector may be configured by user in the future.
  // TODO: need to track the performance when using the assignor in large scale consumers, see
  // https://github.com/skiptests/astraea/pull/1162#discussion_r1036285677
  private final MetricCollector metricCollector =
      MetricCollector.builder()
          .interval(Duration.ofSeconds(1))
          .expiration(Duration.ofSeconds(15))
          .build();

  /**
   * Perform the group assignment given the members' subscription and ClusterInfo.
   *
   * @param subscriptions Map from the member id to their respective topic subscription.
   * @param metadata Current topic/broker metadata known by consumer.
   * @return Map from each member to the list of topic-partitions assigned to them.
   */
  public abstract Map<String, List<TopicPartition>> assign(
      Map<String, org.astraea.common.consumer.assignor.Subscription> subscriptions,
      ClusterInfo<ReplicaInfo> metadata);

  @Override
  public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {

    // convert Kafka's data structure to ours
    var clusterInfo = ClusterInfo.of(metadata);
    var subscriptionsPerMember =
        org.astraea.common.consumer.assignor.GroupSubscription.from(groupSubscription)
            .groupSubscription();

    // register JMX for metric collector only once.
    if (!register) {
      clusterInfo
          .nodes()
          .forEach(
              node ->
                  metricCollector.registerJmx(
                      node.id(),
                      InetSocketAddress.createUnresolved(
                          node.host(), jmxPortGetter.apply(node.id()).get())));
    }
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

  /**
   * Parse config to get JMX port and cost function type.
   *
   * @param config configuration
   */
  @Override
  public void configure(Configuration config) {
    configure(config.integer(JMX_PORT), PartitionerUtils.parseIdJMXPort(config));
  }

  void configure(Optional<Integer> jmxPortDefault, Map<Integer, Integer> customJmxPort) {
    this.jmxPortGetter = id -> Optional.ofNullable(customJmxPort.get(id)).or(() -> jmxPortDefault);
  }
}
