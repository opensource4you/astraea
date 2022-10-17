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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.admin.Partition;
import org.astraea.common.admin.TopicPartition;

public class GroupHandler implements Handler {
  static final String TOPIC_KEY = "topic";
  static final String INSTANCE_KEY = "instance";
  static final String GROUP_KEY = "group";
  private final AsyncAdmin admin;

  GroupHandler(AsyncAdmin admin) {
    this.admin = admin;
  }

  @Override
  public CompletionStage<Response> delete(Channel channel) {
    if (channel.target().isEmpty()) return CompletableFuture.completedStage(Response.NOT_FOUND);
    var groupId = channel.target().get();
    var shouldDeleteGroup =
        Optional.ofNullable(channel.queries().get(GROUP_KEY))
            .filter(Boolean::parseBoolean)
            .isPresent();
    if (shouldDeleteGroup)
      return admin.deleteGroups(Set.of(groupId)).thenApply(ignored -> Response.OK);

    var shouldDeleteInstance = Objects.nonNull(channel.queries().get(INSTANCE_KEY));
    if (shouldDeleteInstance) {
      var groupInstanceId = channel.queries().get(INSTANCE_KEY);
      return admin
          .deleteInstanceMembers(Map.of(groupId, Set.of(groupInstanceId)))
          .thenApply(ignored -> Response.OK);
    }
    return admin.deleteMembers(Set.of(groupId)).thenApply(ignored -> Response.OK);
  }

  @Override
  public CompletionStage<Response> get(Channel channel) {
    return Optional.ofNullable(channel.queries().get(TOPIC_KEY))
        .map(topic -> CompletableFuture.completedStage(Set.of(topic)))
        .orElseGet(() -> admin.topicNames(true))
        .thenCompose(
            topics ->
                admin
                    .consumerGroupIds()
                    .thenApply(
                        groupIds -> {
                          var availableIds =
                              channel
                                  .target()
                                  .map(
                                      r ->
                                          groupIds.stream()
                                              .filter(id -> id.equals(r))
                                              .collect(Collectors.toSet()))
                                  .orElse(groupIds);
                          if (availableIds.isEmpty() && channel.target().isPresent())
                            throw new NoSuchElementException(
                                "group: " + channel.target().get() + " is nonexistent");
                          return availableIds;
                        })
                    .thenCompose(admin::consumerGroups)
                    .thenCombine(
                        admin
                            .partitions(topics)
                            .thenApply(
                                partitions ->
                                    partitions.stream()
                                        .collect(
                                            Collectors.toMap(
                                                Partition::topicPartition, Function.identity()))),
                        (groups, partitions) ->
                            groups.stream()
                                // if users want to search groups for specify topic only, we remove
                                // the group having no offsets related to specify topic
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
                                                            // gson does not support Optional see
                                                            // https://github.com/google/gson/issues/1102
                                                            entry
                                                                .getKey()
                                                                .groupInstanceId()
                                                                .orElse(null),
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
                                                                                    partitions
                                                                                        .get(tp)
                                                                                        .earliestOffset(),
                                                                                    group
                                                                                        .consumeProgress()
                                                                                        .get(tp),
                                                                                    partitions
                                                                                        .get(tp)
                                                                                        .latestOffset()))
                                                                            : Optional
                                                                                .<OffsetProgress>
                                                                                    empty())
                                                                .filter(Optional::isPresent)
                                                                .map(Optional::get)
                                                                .collect(
                                                                    Collectors
                                                                        .toUnmodifiableList())))
                                                .collect(Collectors.toUnmodifiableList())))
                                .collect(Collectors.toUnmodifiableList())))
        .thenApply(
            groups -> {
              if (channel.target().isPresent() && groups.size() == 1) return groups.get(0);
              return new Groups(groups);
            });
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
    final String groupInstanceId;
    final String clientId;
    final String host;
    final List<OffsetProgress> offsetProgress;

    Member(
        String memberId,
        String groupInstanceId,
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
