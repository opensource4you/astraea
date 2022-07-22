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
package org.astraea.app.admin;

import java.util.Objects;

public class TopicPartitionReplica implements Comparable<TopicPartitionReplica> {

  public static TopicPartitionReplica of(String topic, int partition, int brokerId) {
    return new TopicPartitionReplica(topic, partition, brokerId);
  }

  private final int brokerId;
  private final int partition;
  private final String topic;

  private TopicPartitionReplica(String topic, int partition, int brokerId) {
    this.partition = partition;
    this.topic = topic;
    this.brokerId = brokerId;
  }

  @Override
  public int compareTo(TopicPartitionReplica o) {
    var b = Integer.compare(brokerId, o.brokerId);
    if (b != 0) return b;
    var t = topic.compareTo(o.topic);
    if (t != 0) return t;
    return Integer.compare(partition, o.partition);
  }

  public int brokerId() {
    return brokerId;
  }

  public int partition() {
    return partition;
  }

  public String topic() {
    return topic;
  }

  @Override
  public int hashCode() {
    return Objects.hash(brokerId, topic, partition);
  }

  @Override
  public String toString() {
    return String.format("%d-%s-%d", brokerId, topic, partition);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TopicPartitionReplica that = (TopicPartitionReplica) o;
    return brokerId == that.brokerId
        && partition == that.partition
        && Objects.equals(topic, that.topic);
  }
}
