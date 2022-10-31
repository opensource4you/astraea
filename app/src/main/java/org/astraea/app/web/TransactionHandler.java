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
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.astraea.common.admin.Admin;

class TransactionHandler implements Handler {

  private final Admin admin;

  TransactionHandler(Admin admin) {
    this.admin = admin;
  }

  @Override
  public CompletionStage<Response> get(Channel channel) {
    return admin
        .transactionIds()
        .thenApply(
            ids -> {
              var availableIds =
                  channel.target().map(Set::of).orElse(ids).stream()
                      .filter(ids::contains)
                      .collect(Collectors.toSet());
              if (availableIds.isEmpty() && channel.target().isPresent())
                throw new NoSuchElementException(
                    "transaction id: " + channel.target().get() + " is nonexistent");
              return availableIds;
            })
        .thenCompose(admin::transactions)
        .thenApply(
            transactions ->
                transactions.stream()
                    .map(t -> new Transaction(t.transactionId(), t))
                    .collect(Collectors.toUnmodifiableList()))
        .thenApply(
            transactions -> {
              if (channel.target().isPresent() && transactions.size() == 1)
                return transactions.get(0);
              return new Transactions(transactions);
            });
  }

  static class TopicPartition implements Response {
    final String topic;
    final int partition;

    TopicPartition(org.astraea.common.admin.TopicPartition tp) {
      this.topic = tp.topic();
      this.partition = tp.partition();
    }
  }

  static class Transaction implements Response {
    final String id;
    final int coordinatorId;
    final String state;
    final long producerId;
    final int producerEpoch;
    final long transactionTimeoutMs;
    final Set<TopicPartition> topicPartitions;

    Transaction(String id, org.astraea.common.admin.Transaction transaction) {
      this.id = id;
      this.coordinatorId = transaction.coordinatorId();
      this.state = transaction.state().toString();
      this.producerId = transaction.producerId();
      this.producerEpoch = transaction.producerEpoch();
      this.transactionTimeoutMs = transaction.transactionTimeoutMs();
      this.topicPartitions =
          transaction.topicPartitions().stream()
              .map(TopicPartition::new)
              .collect(Collectors.toSet());
    }
  }

  static class Transactions implements Response {
    final List<Transaction> transactions;

    Transactions(List<Transaction> transactions) {
      this.transactions = transactions;
    }
  }
}
