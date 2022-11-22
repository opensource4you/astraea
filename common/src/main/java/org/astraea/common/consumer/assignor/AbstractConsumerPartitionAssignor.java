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

import java.util.List;
import java.util.Map;

import org.astraea.common.Configuration;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;

/**
 * Abstract assignor implementation which does some common work (e.g., configuration).
 */
public abstract class AbstractConsumerPartitionAssignor implements ConsumerPartitionAssignor {
    public static final String JMX_PORT = "jmx.port";

    /**
     * Perform the group assignment given the members' subscription and ClusterInfo.
     * @param subscriptions Map from the member id to their respective topic subscription.
     * @param metadata Current topic/broker metadata known by consumer.
     * @return Map from each member to the list of topic-partitions assigned to them.
     */
  public abstract Map<String, List<TopicPartition>> assign(
      Map<String, org.astraea.common.consumer.assignor.Subscription> subscriptions,
      ClusterInfo<ReplicaInfo> metadata);

  @Override
  public void configure(Configuration config){

  }

}
