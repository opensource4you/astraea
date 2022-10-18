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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Partition;
import org.astraea.common.admin.TopicPartition;

public class GroupHandler implements Handler {
  static final String TOPIC_KEY = "topic";
  static final String INSTANCE_KEY = "instance";
  static final String GROUP_KEY = "group";
  private final Admin admin;

  GroupHandler(Admin admin) {
    this.admin = admin;
  }

  Set<String> groupIds(Optional<String> target) {
    return Handler.compare(admin.consumerGroupIds(), target);
  }

  @Override
  public CompletionStage<Response> delete(Channel channel) {
    if (channel.target().isEmpty()) return CompletableFuture.completedStage(Response.NOT_FOUND);
    var groupId = channel.target().get();
    var shouldDeleteGroup =
        Optional.ofNullable(channel.queries().get(GROUP_KEY))
            .filter(Boolean::parseBoolean)
            .isPresent();
    if (shouldDeleteGroup) {
      admin.removeAllMembers(groupId);
      admin.removeGroup(groupId);
      return CompletableFuture.completedStage(Response.OK);
    }

    var shouldDeleteInstance = Objects.nonNull(channel.queries().get(INSTANCE_KEY));
    if (shouldDeleteInstance) {
      var groupInstanceId = channel.queries().get(INSTANCE_KEY);
      var instanceExisted =
          admin.consumerGroups(Set.of(groupId)).stream()
              .filter(g -> g.groupId().equals(groupId))
              .flatMap(g -> g.assignment().keySet().stream())
              .anyMatch(x -> x.groupInstanceId().filter(groupInstanceId::equals).isPresent());
      if (instanceExisted) admin.removeStaticMembers(groupId, Set.of(groupInstanceId));
    } else {
      admin.removeAllMembers(groupId);
    }
    return CompletableFuture.completedStage(Response.OK);
  }

  @Override
  public CompletionStage<Response> get(Channel channel) {
    var topics =
        channel.queries().containsKey(TOPIC_KEY)
            ? Set.of(channel.queries().get(TOPIC_KEY))
            : admin.topicNames();
    var consumerGroups = admin.consumerGroups(groupIds(channel.target()));
    var partitions =
        admin.partitions(topics).stream()
            .collect(Collectors.toMap(Partition::topicPartition, Function.identity()));

    var groups =
        consumerGroups.stream()
            // if users want to search groups for specify topic only, we remove the group having no
            // offsets related to specify topic
            .filter(
                g ->
                    !channel.queries().containsKey(TOPIC_KEY)
                        || g.consumeProgress().keySet().stream()
                            .map(TopicPartition::topic)
                            .anyMatch(topics::contains))
            .map(
                group ->
                    new Group(
                        group.groupId(),
                        group.assignment().entrySet().stream()
                            .map(
                                entry ->
                                    new Member(
                                        entry.getKey().memberId(),
                                        entry.getKey().groupInstanceId(),
                                        entry.getKey().clientId(),
                                        entry.getKey().host(),
                                        entry.getValue().stream()
                                            .map(
                                                tp ->
                                                    partitions.containsKey(tp)
                                                            && group
                                                                .consumeProgress()
                                                                .containsKey(tp)
                                                        ? Optional.of(
                                                            new OffsetProgress(
                                                                tp.topic(),
                                                                tp.partition(),
                                                                partitions.get(tp).earliestOffset(),
                                                                group.consumeProgress().get(tp),
                                                                partitions.get(tp).latestOffset()))
                                                        : Optional.<OffsetProgress>empty())
                                            .filter(Optional::isPresent)
                                            .map(Optional::get)
                                            .collect(Collectors.toUnmodifiableList())))
                            .collect(Collectors.toUnmodifiableList())))
            .collect(Collectors.toUnmodifiableList());

    if (channel.target().isPresent() && groups.size() == 1)
      return CompletableFuture.completedFuture(groups.get(0));
    return CompletableFuture.completedFuture(new Groups(groups));
  }

  static class OffsetProgress implements Response {
    final String topic;
    final int partitionId;
    final long earliest;
    final long current;
    final long latest;

    OffsetProgress(String topicName, int partitionId, long earliest, long current, long latest) {
      this.topic = topicName;
      this.partitionId = partitionId;
      this.earliest = earliest;
      this.current = current;
      this.latest = latest;
    }
  }

  static class Member implements Response {
    final String memberId;
    final Optional<String> groupInstanceId;
    final String clientId;
    final String host;
    final List<OffsetProgress> offsetProgress;

    public Member() {
      memberId = null;
      groupInstanceId = Optional.empty();
      clientId = null;
      host = null;
      offsetProgress = null;
    }

    Member(
        String memberId,
        Optional<String> groupInstanceId,
        String clientId,
        String host,
        List<OffsetProgress> offsetProgress) {
      this.memberId = memberId;
      this.groupInstanceId = groupInstanceId;
      this.clientId = clientId;
      this.host = host;
      this.offsetProgress = offsetProgress;
    }
  }

  static class Group implements Response {
    final String groupId;
    final List<Member> members;

    Group(String groupId, List<Member> members) {
      this.groupId = groupId;
      this.members = members;
    }
  }

  static class Groups implements Response {
    final List<Group> groups;

    Groups(List<Group> groups) {
      this.groups = groups;
    }
  }
}
