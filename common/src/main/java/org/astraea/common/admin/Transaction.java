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
import java.util.Set;
import java.util.stream.Collectors;

public class Transaction {

  public static Transaction from(org.apache.kafka.clients.admin.TransactionDescription td) {
    return new Transaction(
        td.coordinatorId(),
        TransactionState.from(td.state()),
        td.producerId(),
        td.producerEpoch(),
        td.transactionTimeoutMs(),
        td.topicPartitions().stream().map(TopicPartition::from).collect(Collectors.toSet()));
  }

  private final int coordinatorId;
  private final TransactionState state;
  private final long producerId;
  private final int producerEpoch;
  private final long transactionTimeoutMs;
  private final Set<TopicPartition> topicPartitions;

  public Transaction(
      int coordinatorId,
      TransactionState state,
      long producerId,
      int producerEpoch,
      long transactionTimeoutMs,
      Set<TopicPartition> topicPartitions) {
    this.coordinatorId = coordinatorId;
    this.state = state;
    this.producerId = producerId;
    this.producerEpoch = producerEpoch;
    this.transactionTimeoutMs = transactionTimeoutMs;
    this.topicPartitions = topicPartitions;
  }

  public int coordinatorId() {
    return coordinatorId;
  }

  public TransactionState state() {
    return state;
  }

  public long producerId() {
    return producerId;
  }

  public int producerEpoch() {
    return producerEpoch;
  }

  public long transactionTimeoutMs() {
    return transactionTimeoutMs;
  }

  public Set<TopicPartition> topicPartitions() {
    return topicPartitions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Transaction that = (Transaction) o;
    return coordinatorId == that.coordinatorId
        && producerId == that.producerId
        && producerEpoch == that.producerEpoch
        && transactionTimeoutMs == that.transactionTimeoutMs
        && state == that.state
        && Objects.equals(topicPartitions, that.topicPartitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        coordinatorId, state, producerId, producerEpoch, transactionTimeoutMs, topicPartitions);
  }

  @Override
  public String toString() {
    return "Transaction{"
        + "coordinatorId="
        + coordinatorId
        + ", state="
        + state
        + ", producerId="
        + producerId
        + ", producerEpoch="
        + producerEpoch
        + ", transactionTimeoutMs="
        + transactionTimeoutMs
        + ", topicPartitions="
        + topicPartitions
        + '}';
  }
}
