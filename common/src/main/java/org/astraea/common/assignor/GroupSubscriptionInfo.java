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

import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;

public final class GroupSubscriptionInfo {
  private final Map<String, SubscriptionInfo> subscriptions;

  public GroupSubscriptionInfo(Map<String, SubscriptionInfo> subscriptions) {
    this.subscriptions = subscriptions;
  }

  public Map<String, SubscriptionInfo> groupSubscription() {
    return subscriptions;
  }

  public static GroupSubscriptionInfo from(
      ConsumerPartitionAssignor.GroupSubscription groupSubscription) {
    return new GroupSubscriptionInfo(
        groupSubscription.groupSubscription().entrySet().stream()
            .collect(
                Collectors.toMap(Map.Entry::getKey, e -> SubscriptionInfo.from(e.getValue()))));
  }

  @Override
  public String toString() {
    return "GroupSubscription(" + "subscriptions=" + subscriptions + ")";
  }
}
