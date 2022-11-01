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

import java.util.NoSuchElementException;
import java.util.concurrent.CompletionException;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.producer.Producer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TransactionHandlerTest extends RequireBrokerCluster {

  @Test
  void testListTransactions() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer =
            Producer.builder().bootstrapServers(bootstrapServers()).buildTransactional()) {
      var handler = new TransactionHandler(admin);
      producer.sender().topic(topicName).value(new byte[1]).run().toCompletableFuture().join();

      // wait for all transactions are completed
      Utils.waitFor(
          () -> {
            var result =
                Assertions.assertInstanceOf(
                    TransactionHandler.Transactions.class,
                    handler.get(Channel.EMPTY).toCompletableFuture().join());
            var transaction =
                result.transactions.stream()
                    .filter(t -> t.id.equals(producer.transactionId().get()))
                    .findFirst()
                    .get();
            return transaction.topicPartitions.isEmpty();
          });
    }
  }

  @Test
  void testQueryTransactionId() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer =
            Producer.builder().bootstrapServers(bootstrapServers()).buildTransactional()) {
      var handler = new TransactionHandler(admin);
      producer.sender().topic(topicName).value(new byte[1]).run().toCompletableFuture().join();

      // wait for all transactions are completed
      Utils.waitFor(
          () -> {
            var transaction =
                Assertions.assertInstanceOf(
                    TransactionHandler.Transaction.class,
                    handler
                        .get(Channel.ofTarget(producer.transactionId().get()))
                        .toCompletableFuture()
                        .join());
            return transaction.topicPartitions.isEmpty();
          });
    }
  }

  @Test
  void queryNonexistentTransactionId() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer =
            Producer.builder().bootstrapServers(bootstrapServers()).buildTransactional()) {
      var handler = new TransactionHandler(admin);
      producer.sender().topic(topicName).value(new byte[1]).run().toCompletableFuture().join();

      Assertions.assertInstanceOf(
          NoSuchElementException.class,
          Assertions.assertThrows(
                  CompletionException.class,
                  () ->
                      handler
                          .get(Channel.ofTarget(Utils.randomString(10)))
                          .toCompletableFuture()
                          .join())
              .getCause());
    }
  }
}
