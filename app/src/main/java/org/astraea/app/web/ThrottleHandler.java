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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.DataRate;
import org.astraea.common.EnumInfo;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;

public class ThrottleHandler implements Handler {
  private final Admin admin;

  public ThrottleHandler(Admin admin) {
    this.admin = admin;
  }

  @Override
  public Response get(Channel channel) {
    return get();
  }

  private Response get() {
    final var brokers =
        admin.brokers().entrySet().stream()
            .map(
                entry -> {
                  final var egress =
                      entry
                          .getValue()
                          .value("leader.replication.throttled.rate")
                          .map(Long::valueOf)
                          .orElse(null);
                  final var ingress =
                      entry
                          .getValue()
                          .value("follower.replication.throttled.rate")
                          .map(Long::valueOf)
                          .orElse(null);
                  return new BrokerThrottle(entry.getKey(), ingress, egress);
                })
            .collect(Collectors.toUnmodifiableSet());
    final var topicConfigs = admin.topics();
    final var leaderTargets =
        topicConfigs.entrySet().stream()
            .map(
                entry ->
                    toReplicaSet(
                        entry.getKey(),
                        entry.getValue().value("leader.replication.throttled.replicas").orElse("")))
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableSet());
    final var followerTargets =
        topicConfigs.entrySet().stream()
            .map(
                entry ->
                    toReplicaSet(
                        entry.getKey(),
                        entry
                            .getValue()
                            .value("follower.replication.throttled.replicas")
                            .orElse("")))
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableSet());

    return new ThrottleSetting(brokers, simplify(leaderTargets, followerTargets));
  }

  @Override
  public Response post(Channel channel) {
    var brokerToUpdate =
        channel
            .request()
            .<Collection<BrokerThrottle>>get(
                "brokers",
                TypeToken.getParameterized(Collection.class, BrokerThrottle.class).getType())
            .orElse(List.of());
    var topics =
        channel
            .request()
            .<Collection<TopicThrottle>>get(
                "topics",
                TypeToken.getParameterized(Collection.class, TopicThrottle.class).getType())
            .orElse(List.of());

    final var throttler = admin.replicationThrottler();
    // ingress
    throttler.ingress(
        brokerToUpdate.stream()
            .filter(broker -> broker.ingress != null)
            .collect(
                Collectors.toUnmodifiableMap(
                    broker -> broker.id, broker -> DataRate.Byte.of(broker.ingress).perSecond())));
    // egress
    throttler.egress(
        brokerToUpdate.stream()
            .filter(broker -> broker.egress != null)
            .collect(
                Collectors.toUnmodifiableMap(
                    broker -> broker.id, broker -> DataRate.Byte.of(broker.egress).perSecond())));

    topics.forEach(
        topic -> {
          //noinspection ConstantConditions, the deserialization result must leave this value null
          if (topic.name == null)
            throw new IllegalArgumentException("The 'name' key of topic throttle must be given");
          else if (topic.partition == null && topic.broker == null && topic.type == null) {
            throttler.throttle(topic.name);
          } else if (topic.partition != null && topic.broker == null && topic.type == null) {
            throttler.throttle(TopicPartition.of(topic.name, topic.partition));
          } else if (topic.partition != null && topic.broker != null && topic.type == null) {
            throttler.throttle(TopicPartitionReplica.of(topic.name, topic.partition, topic.broker));
          } else if (topic.partition != null && topic.broker != null && topic.type != null) {
            var replica = TopicPartitionReplica.of(topic.name, topic.partition, topic.broker);
            if (topic.type.equals("leader")) throttler.throttleLeader(replica);
            else if (topic.type.equals("follower")) throttler.throttleFollower(replica);
            else throw new IllegalArgumentException("Unknown throttle type: " + topic.type);
          } else {
            throw new IllegalArgumentException(
                "The TopicThrottle argument is not supported: " + topic);
          }
        });

    var affectedResources = throttler.apply();
    var affectedBrokers =
        Stream.concat(
                affectedResources.ingress().keySet().stream(),
                affectedResources.egress().keySet().stream())
            .distinct()
            .map(
                broker ->
                    BrokerThrottle.of(
                        broker,
                        affectedResources.ingress().get(broker),
                        affectedResources.egress().get(broker)))
            .collect(Collectors.toUnmodifiableList());
    var affectedTopics = simplify(affectedResources.leaders(), affectedResources.followers());
    return new ThrottleSetting(affectedBrokers, affectedTopics);
  }

  @Override
  public Response delete(Channel channel) {
    if (channel.queries().containsKey("topic")) {
      var topic =
          new TopicThrottle(
              channel.queries().get("topic"),
              Optional.ofNullable(channel.queries().get("partition"))
                  .map(Integer::parseInt)
                  .orElse(null),
              Optional.ofNullable(channel.queries().get("replica"))
                  .map(Integer::parseInt)
                  .orElse(null),
              channel.queries().get("type") == null
                  ? null
                  : Arrays.stream(LogIdentity.values())
                      .filter(x -> x.name().equals(channel.queries().get("type")))
                      .findFirst()
                      .orElseThrow(IllegalArgumentException::new));

      if (topic.partition == null && topic.broker == null && topic.type == null)
        admin.clearReplicationThrottle(topic.name);
      else if (topic.partition != null && topic.broker == null && topic.type == null)
        admin.clearReplicationThrottle(TopicPartition.of(topic.name, topic.partition));
      else if (topic.partition != null && topic.broker != null && topic.type == null)
        admin.clearReplicationThrottle(
            TopicPartitionReplica.of(topic.name, topic.partition, topic.broker));
      else if (topic.partition != null && topic.broker != null && topic.type.equals("leader"))
        admin.clearLeaderReplicationThrottle(
            TopicPartitionReplica.of(topic.name, topic.partition, topic.broker));
      else if (topic.partition != null && topic.broker != null && topic.type.equals("follower"))
        admin.clearFollowerReplicationThrottle(
            TopicPartitionReplica.of(topic.name, topic.partition, topic.broker));
      else
        throw new IllegalArgumentException("The argument is not supported: " + channel.queries());

      return Response.ACCEPT;
    } else if (channel.queries().containsKey("broker")) {
      var broker = Integer.parseInt(channel.queries().get("broker"));
      var bandwidth = channel.queries().get("type").split("\\+");
      for (String target : bandwidth) {
        if (target.equals("ingress")) admin.clearIngressReplicationThrottle(Set.of(broker));
        else if (target.equals("egress")) admin.clearEgressReplicationThrottle(Set.of(broker));
        else throw new IllegalArgumentException("Unknown clear target: " + target);
      }
      return Response.ACCEPT;
    } else {
      return Response.BAD_REQUEST;
    }
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
    final Long ingress;
    final Long egress;

    static BrokerThrottle of(int id, DataRate ingress, DataRate egress) {
      return new BrokerThrottle(
          id,
          (ingress != null) ? ((long) ingress.byteRate()) : (null),
          (egress != null) ? ((long) egress.byteRate()) : (null));
    }

    BrokerThrottle(int id, Long ingress, Long egress) {
      this.id = id;
      this.ingress = ingress;
      this.egress = egress;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BrokerThrottle that = (BrokerThrottle) o;
      return id == that.id
          && Objects.equals(ingress, that.ingress)
          && Objects.equals(egress, that.egress);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, ingress, egress);
    }

    @Override
    public String toString() {
      return "BrokerThrottle{"
          + "broker="
          + id
          + ", ingress="
          + ingress
          + ", egress="
          + egress
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
  }
}
