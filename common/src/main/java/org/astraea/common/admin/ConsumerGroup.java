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
package org.astraea.common.admin;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ConsumerGroup {
  private final String groupId;
  private final List<Member> activeMembers;
  private final Map<TopicPartition, Long> consumeProgress;
  private final Map<Member, Set<TopicPartition>> assignment;

  public ConsumerGroup(
      String groupId,
      List<Member> activeMembers,
      Map<TopicPartition, Long> consumeProgress,
      Map<Member, Set<TopicPartition>> assignment) {
    this.groupId = Objects.requireNonNull(groupId);
    this.activeMembers = List.copyOf(activeMembers);
    this.consumeProgress = Map.copyOf(consumeProgress);
    this.assignment = Map.copyOf(assignment);
  }

  public String groupId() {
    return groupId;
  }

  public Map<Member, Set<TopicPartition>> assignment() {
    return assignment;
  }

  public Map<TopicPartition, Long> consumeProgress() {
    return consumeProgress;
  }

  public List<Member> activeMembers() {
    return activeMembers;
  }

  @Override
  public String toString() {
    return "ConsumerGroup{"
        + "groupId='"
        + groupId
        + '\''
        + ", activeMembers="
        + activeMembers
        + ", consumeProgress="
        + consumeProgress
        + ", assignment="
        + assignment
        + '}';
  }
}
