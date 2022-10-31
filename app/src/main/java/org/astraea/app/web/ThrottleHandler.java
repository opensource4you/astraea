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
package org.astraea.app.web;

import com.google.gson.reflect.TypeToken;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.EnumInfo;
import org.astraea.common.FutureUtils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.BrokerConfigs;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.TopicConfigs;
import org.astraea.common.admin.TopicPartitionReplica;

public class ThrottleHandler implements Handler {
  private final Admin admin;

  public ThrottleHandler(Admin admin) {
    this.admin = admin;
  }

  @Override
  public CompletionStage<ThrottleSetting> get(Channel channel) {
    return FutureUtils.combine(
        admin.brokers(),
        admin.topicNames(false).thenCompose(admin::topics),
        (brokers, topics) ->
            new ThrottleSetting(
                brokers.stream()
                    .map(
                        node ->
                            new BrokerThrottle(
                                node.id(),
                                node.config()
                                    .value(BrokerConfigs.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG)
                                    .map(Long::valueOf)
                                    .orElse(null),
                                node.config()
                                    .value(BrokerConfigs.LEADER_REPLICATION_THROTTLED_RATE_CONFIG)
                                    .map(Long::valueOf)
                                    .orElse(null)))
                    .filter(b -> b.leader != null || b.follower != null)
                    .collect(Collectors.toUnmodifiableSet()),
                simplify(
                    topics.stream()
                        .map(
                            topic ->
                                toReplicaSet(
                                    topic.name(),
                                    topic
                                        .config()
                                        .value(
                                            TopicConfigs
                                                .LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG)
                                        .orElse("")))
                        .flatMap(Collection::stream)
                        .collect(Collectors.toUnmodifiableSet()),
                    topics.stream()
                        .map(
                            topic ->
                                toReplicaSet(
                                    topic.name(),
                                    topic
                                        .config()
                                        .value(
                                            TopicConfigs
                                                .FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)
                                        .orElse("")))
                        .flatMap(Collection::stream)
                        .collect(Collectors.toUnmodifiableSet()))));
  }

  @Override
  public CompletionStage<Response> post(Channel channel) {
    var topicToAppends =
        admin
            .nodeInfos()
            .thenApply(
                nodeInfos -> nodeInfos.stream().map(NodeInfo::id).collect(Collectors.toSet()))
            .thenCompose(admin::topicPartitionReplicas)
            .thenApply(
                replicas ->
                    channel
                        .request()
                        .<Collection<TopicThrottle>>get(
                            "topics",
                            TypeToken.getParameterized(Collection.class, TopicThrottle.class)
                                .getType())
                        .orElse(List.of())
                        .stream()
                        .flatMap(
                            t -> {
                              var keys =
                                  Stream.of(
                                          TopicConfigs
                                              .FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
                                          TopicConfigs.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG)
                                      .filter(
                                          key ->
                                              t.type == null
                                                  || (t.type.equals("leader")
                                                      && key.equals(
                                                          TopicConfigs
                                                              .LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG))
                                                  || (t.type.equals("follower")
                                                      && key.equals(
                                                          TopicConfigs
                                                              .FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)))
                                      .collect(Collectors.toSet());
                              return replicas.stream()
                                  .filter(
                                      r ->
                                          (t.name == null || r.topic().equals(t.name))
                                              && (t.partition == null
                                                  || r.partition() == t.partition)
                                              && (t.broker == null || r.brokerId() == t.broker))
                                  .collect(Collectors.groupingBy(TopicPartitionReplica::topic))
                                  .entrySet()
                                  .stream()
                                  .map(
                                      entry ->
                                          Map.entry(
                                              entry.getKey(),
                                              keys.stream()
                                                  .collect(
                                                      Collectors.toMap(
                                                          key -> key,
                                                          ignored ->
                                                              entry.getValue().stream()
                                                                  .map(
                                                                      r ->
                                                                          r.partition()
                                                                              + ":"
                                                                              + r.brokerId())
                                                                  .collect(
                                                                      Collectors.joining(","))))));
                            })
                        .collect(
                            Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (l, r) -> {
                                  // Merge the duplicate topic requests by order
                                  var merged = new HashMap<>(l);
                                  merged.putAll(r);
                                  return merged;
                                })));

    Map<Integer, Map<String, String>> brokerToSets =
        channel
            .request()
            .<Collection<BrokerThrottle>>get(
                "brokers",
                TypeToken.getParameterized(Collection.class, BrokerThrottle.class).getType())
            .orElse(List.of())
            .stream()
            .collect(
                Collectors.toMap(
                    b -> b.id,
                    b -> {
                      var result = new HashMap<String, String>();
                      if (b.follower != null)
                        result.put(
                            BrokerConfigs.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG,
                            String.valueOf(b.follower));
                      if (b.leader != null)
                        result.put(
                            BrokerConfigs.LEADER_REPLICATION_THROTTLED_RATE_CONFIG,
                            String.valueOf(b.leader));
                      return result;
                    }));

    return topicToAppends
        .thenCompose(admin::appendTopicConfigs)
        .thenCompose(ignored -> admin.setBrokerConfigs(brokerToSets))
        .thenApply(ignored -> Response.ACCEPT);
  }

  @Override
  public CompletionStage<Response> delete(Channel channel) {

    var topicToSubtracts =
        admin
            .nodeInfos()
            .thenApply(
                nodeInfos -> nodeInfos.stream().map(NodeInfo::id).collect(Collectors.toSet()))
            .thenCompose(admin::topicPartitionReplicas)
            .thenApply(
                replicas -> {
                  var keys =
                      Stream.of(
                              TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
                              TopicConfigs.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG)
                          .filter(
                              key ->
                                  !channel.queries().containsKey("type")
                                      || (channel.queries().get("type").equals("follower")
                                          && key.equals(
                                              TopicConfigs
                                                  .FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG))
                                      || (channel.queries().get("type").equals("leader")
                                          && key.equals(
                                              TopicConfigs
                                                  .LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG)))
                          .collect(Collectors.toSet());

                  return replicas.stream()
                      .filter(
                          r ->
                              (!channel.queries().containsKey("topic")
                                      || channel.queries().get("topic").equals(r.topic()))
                                  && (!channel.queries().containsKey("partition")
                                      || Integer.parseInt(channel.queries().get("partition"))
                                          == r.partition())
                                  && (!channel.queries().containsKey("replica")
                                      || Integer.parseInt(channel.queries().get("replica"))
                                          == r.brokerId()))
                      .collect(Collectors.groupingBy(TopicPartitionReplica::topic))
                      .entrySet()
                      .stream()
                      .map(
                          entry ->
                              Map.entry(
                                  entry.getKey(),
                                  keys.stream()
                                      .collect(
                                          Collectors.toMap(
                                              key -> key,
                                              ignored ->
                                                  entry.getValue().stream()
                                                      .map(r -> r.partition() + ":" + r.brokerId())
                                                      .collect(Collectors.joining(","))))))
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                });

    var brokerToUnset =
        admin
            .nodeInfos()
            .thenApply(
                ns ->
                    ns.stream()
                        .map(NodeInfo::id)
                        .filter(
                            id ->
                                !channel.queries().containsKey("broker")
                                    || Integer.parseInt(channel.queries().get("broker")) == id)
                        .collect(Collectors.toSet()))
            .thenApply(
                ids ->
                    ids.stream()
                        .collect(
                            Collectors.toMap(
                                id -> id,
                                id ->
                                    Stream.of(
                                            BrokerConfigs
                                                .FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG,
                                            BrokerConfigs.LEADER_REPLICATION_THROTTLED_RATE_CONFIG)
                                        .filter(
                                            key ->
                                                !channel.queries().containsKey("type")
                                                    || Arrays.stream(
                                                            channel
                                                                .queries()
                                                                .get("type")
                                                                .split("\\+"))
                                                        .flatMap(
                                                            t ->
                                                                t.equals("follower")
                                                                    ? Stream.of(
                                                                        BrokerConfigs
                                                                            .FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG)
                                                                    : t.equals("leader")
                                                                        ? Stream.of(
                                                                            BrokerConfigs
                                                                                .LEADER_REPLICATION_THROTTLED_RATE_CONFIG)
                                                                        : Stream.of())
                                                        .collect(Collectors.toSet())
                                                        .contains(key))
                                        .collect(Collectors.toSet()))));

    return topicToSubtracts
        .thenCompose(admin::subtractTopicConfigs)
        .thenCompose(ignored -> brokerToUnset.thenCompose(admin::unsetBrokerConfigs))
        .thenApply(ignored -> Response.ACCEPT);
  }

  /**
   * break apart the {@code throttled.replica} string setting into a set of topic/partition/replicas
   */
  private Set<TopicPartitionReplica> toReplicaSet(String topic, String throttledReplicas) {
    if (throttledReplicas.isEmpty()) return Set.of();

    // TODO: support for wildcard throttle might be implemented in the future, see
    // https://github.com/skiptests/astraea/issues/625
    if (throttledReplicas.equals("*"))
      throw new UnsupportedOperationException("This API doesn't support wildcard throttle");

    return Arrays.stream(throttledReplicas.split(","))
        .map(pair -> pair.split(":"))
        .map(
            pair ->
                TopicPartitionReplica.of(
                    topic, Integer.parseInt(pair[0]), Integer.parseInt(pair[1])))
        .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * Given a series of leader/follower throttle config, this method attempts to reduce its size into
   * the simplest form by merging any targets with a common topic/partition/replica scope throttle
   * target.
   */
  private Set<TopicThrottle> simplify(
      Set<TopicPartitionReplica> leaders, Set<TopicPartitionReplica> followers) {
    return Stream.concat(leaders.stream(), followers.stream())
        .distinct()
        .map(
            r -> {
              if (leaders.contains(r) && followers.contains(r))
                return new TopicThrottle(r.topic(), r.partition(), r.brokerId(), null);
              if (leaders.contains(r))
                return new TopicThrottle(
                    r.topic(), r.partition(), r.brokerId(), LogIdentity.leader);
              return new TopicThrottle(
                  r.topic(), r.partition(), r.brokerId(), LogIdentity.follower);
            })
        .collect(Collectors.toUnmodifiableSet());
  }

  static class ThrottleSetting implements Response {

    final Collection<BrokerThrottle> brokers;
    final Collection<TopicThrottle> topics;

    ThrottleSetting(Collection<BrokerThrottle> brokers, Collection<TopicThrottle> topics) {
      this.brokers = brokers;
      this.topics = topics;
    }
  }

  static class BrokerThrottle {
    final int id;
    final Long follower;
    final Long leader;

    BrokerThrottle(int id, Long ingress, Long egress) {
      this.id = id;
      this.follower = ingress;
      this.leader = egress;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BrokerThrottle that = (BrokerThrottle) o;
      return id == that.id
          && Objects.equals(follower, that.follower)
          && Objects.equals(leader, that.leader);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, follower, leader);
    }

    @Override
    public String toString() {
      return "BrokerThrottle{"
          + "broker="
          + id
          + ", ingress="
          + follower
          + ", egress="
          + leader
          + '}';
    }
  }

  static class TopicThrottle {
    final String name;
    final Integer partition;
    final Integer broker;
    final String type;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TopicThrottle that = (TopicThrottle) o;
      return Objects.equals(name, that.name)
          && Objects.equals(partition, that.partition)
          && Objects.equals(broker, that.broker)
          && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, partition, broker, type);
    }

    TopicThrottle(String name, Integer partition, Integer broker, LogIdentity identity) {
      this.name = Objects.requireNonNull(name);
      this.partition = partition;
      this.broker = broker;
      this.type = (identity == null) ? null : identity.name();
    }

    @Override
    public String toString() {
      return "ThrottleTarget{"
          + "name='"
          + name
          + '\''
          + ", partition="
          + partition
          + ", broker="
          + broker
          + ", type="
          + type
          + '}';
    }
  }

  enum LogIdentity implements EnumInfo {
    leader,
    follower;

    public static LogIdentity ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(LogIdentity.class, alias);
    }

    @Override
    public String alias() {
      return name();
    }

    @Override
    public String toString() {
      return alias();
    }
  }
}
