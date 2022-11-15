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

import java.util.Objects;

public final class TopicPartition implements Comparable<TopicPartition> {

  public static TopicPartition from(org.apache.kafka.common.TopicPartition tp) {
    return TopicPartition.of(tp.topic(), tp.partition());
  }

  public static org.apache.kafka.common.TopicPartition to(TopicPartition tp) {
    return new org.apache.kafka.common.TopicPartition(tp.topic(), tp.partition());
  }

  public static TopicPartition of(String topic, String partition) {
    return of(topic + "-" + partition);
  }

  public static TopicPartition of(String value) {
    var lhs = value.lastIndexOf("-");
    if (lhs <= 0 || lhs == value.length() - 1)
      throw new IllegalArgumentException(
          value + " has illegal format. It should be {topic}-{partition}");
    try {
      return TopicPartition.of(value.substring(0, lhs), Integer.parseInt(value.substring(lhs + 1)));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("partition id must be number");
    }
  }

  public static TopicPartition of(String topic, int partition) {
    return new TopicPartition(topic, partition);
  }

  private final int partition;
  private final String topic;

  private TopicPartition(String topic, int partition) {
    this.partition = partition;
    this.topic = topic;
  }

  @Override
  public int compareTo(TopicPartition o) {
    var r = topic.compareTo(o.topic);
    if (r != 0) return r;
    return Integer.compare(partition, o.partition);
  }

  public int partition() {
    return partition;
  }

  public String topic() {
    return topic;
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, partition);
  }

  @Override
  public String toString() {
    return topic + "-" + partition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TopicPartition that = (TopicPartition) o;
    return partition == that.partition && Objects.equals(topic, that.topic);
  }
}
