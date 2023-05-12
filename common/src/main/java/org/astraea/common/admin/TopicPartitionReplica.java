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

public record TopicPartitionReplica(String topic, int partition, int brokerId)
    implements Comparable<TopicPartitionReplica> {

  public static org.apache.kafka.common.TopicPartitionReplica to(TopicPartitionReplica replica) {
    return new org.apache.kafka.common.TopicPartitionReplica(
        replica.topic, replica.partition, replica.brokerId);
  }

  public static TopicPartitionReplica of(String topic, int partition, int brokerId) {
    return new TopicPartitionReplica(topic, partition, brokerId);
  }

  @Override
  public int compareTo(TopicPartitionReplica o) {
    var b = Integer.compare(brokerId, o.brokerId);
    if (b != 0) return b;
    var t = topic.compareTo(o.topic);
    if (t != 0) return t;
    return Integer.compare(partition, o.partition);
  }

  public TopicPartition topicPartition() {
    return TopicPartition.of(topic, partition);
  }
}
