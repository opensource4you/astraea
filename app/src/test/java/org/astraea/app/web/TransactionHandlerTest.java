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
import java.util.concurrent.ExecutionException;
import org.astraea.common.Utils;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.producer.Producer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TransactionHandlerTest extends RequireBrokerCluster {

  @Test
  void testListTransactions() throws ExecutionException, InterruptedException {
    var topicName = Utils.randomString(10);
    try (var admin = AsyncAdmin.of(bootstrapServers());
        var producer =
            Producer.builder().bootstrapServers(bootstrapServers()).buildTransactional()) {
      var handler = new TransactionHandler(admin);
      producer.sender().topic(topicName).value(new byte[1]).run().toCompletableFuture().get();

      // wait for all transactions are completed
      Utils.waitFor(
          () -> {
            var result =
                Assertions.assertInstanceOf(
                    TransactionHandler.Transactions.class,
                    Utils.packException(
                        () -> handler.get(Channel.EMPTY).toCompletableFuture().get()));
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
  void testQueryTransactionId() throws ExecutionException, InterruptedException {
    var topicName = Utils.randomString(10);
    try (var admin = AsyncAdmin.of(bootstrapServers());
        var producer =
            Producer.builder().bootstrapServers(bootstrapServers()).buildTransactional()) {
      var handler = new TransactionHandler(admin);
      producer.sender().topic(topicName).value(new byte[1]).run().toCompletableFuture().get();

      // wait for all transactions are completed
      Utils.waitFor(
          () -> {
            var transaction =
                Assertions.assertInstanceOf(
                    TransactionHandler.Transaction.class,
                    Utils.packException(
                        () ->
                            handler
                                .get(Channel.ofTarget(producer.transactionId().get()))
                                .toCompletableFuture()
                                .get()));
            return transaction.topicPartitions.isEmpty();
          });
    }
  }

  @Test
  void queryNonexistentTransactionId() throws ExecutionException, InterruptedException {
    var topicName = Utils.randomString(10);
    try (var admin = AsyncAdmin.of(bootstrapServers());
        var producer =
            Producer.builder().bootstrapServers(bootstrapServers()).buildTransactional()) {
      var handler = new TransactionHandler(admin);
      producer.sender().topic(topicName).value(new byte[1]).run().toCompletableFuture().get();

      Assertions.assertInstanceOf(
          NoSuchElementException.class,
          Assertions.assertThrows(
                  ExecutionException.class,
                  () ->
                      handler
                          .get(Channel.ofTarget(Utils.randomString(10)))
                          .toCompletableFuture()
                          .get())
              .getCause());
    }
  }
}
