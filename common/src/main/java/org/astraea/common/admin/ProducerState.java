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

public final class ProducerState {

  public static ProducerState of(
      org.apache.kafka.common.TopicPartition tp,
      org.apache.kafka.clients.admin.ProducerState state) {
    return new ProducerState(
        tp.topic(),
        tp.partition(),
        state.producerId(),
        state.producerEpoch(),
        state.lastSequence(),
        state.lastTimestamp());
  }

  private final String topic;

  private final int partition;
  private final long producerId;
  private final int producerEpoch;
  private final int lastSequence;
  private final long lastTimestamp;

  public ProducerState(
      String topic,
      int partition,
      long producerId,
      int producerEpoch,
      int lastSequence,
      long lastTimestamp) {
    this.topic = topic;
    this.partition = partition;
    this.producerId = producerId;
    this.producerEpoch = producerEpoch;
    this.lastSequence = lastSequence;
    this.lastTimestamp = lastTimestamp;
  }

  public TopicPartition topicPartition() {
    return TopicPartition.of(topic, partition);
  }

  public String topic() {
    return topic;
  }

  public int partition() {
    return partition;
  }

  public long producerId() {
    return producerId;
  }

  public int producerEpoch() {
    return producerEpoch;
  }

  public int lastSequence() {
    return lastSequence;
  }

  public long lastTimestamp() {
    return lastTimestamp;
  }

  @Override
  public String toString() {
    return "ProducerState{"
        + "topic='"
        + topic
        + '\''
        + ", partition="
        + partition
        + ", producerId="
        + producerId
        + ", producerEpoch="
        + producerEpoch
        + ", lastSequence="
        + lastSequence
        + ", lastTimestamp="
        + lastTimestamp
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ProducerState that = (ProducerState) o;
    return partition == that.partition
        && producerId == that.producerId
        && producerEpoch == that.producerEpoch
        && lastSequence == that.lastSequence
        && lastTimestamp == that.lastTimestamp
        && topic.equals(that.topic);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, partition, producerId, producerEpoch, lastSequence, lastTimestamp);
  }
}
