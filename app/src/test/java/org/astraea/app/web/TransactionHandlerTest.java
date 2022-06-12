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

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.astraea.app.admin.Admin;
import org.astraea.app.common.Utils;
import org.astraea.app.producer.Producer;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TransactionHandlerTest extends RequireBrokerCluster {

  @Test
  void testListTransactions() throws ExecutionException, InterruptedException {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer =
            Producer.builder().bootstrapServers(bootstrapServers()).buildTransactional()) {
      var handler = new TransactionHandler(admin);
      producer.sender().topic(topicName).value(new byte[1]).run().toCompletableFuture().get();

      var result =
          Assertions.assertInstanceOf(
              TransactionHandler.Transactions.class, handler.get(Optional.empty(), Map.of()));
      var transaction =
          result.transactions.stream()
              .filter(t -> t.id.equals(producer.transactionId().get()))
              .findFirst()
              .get();
      Assertions.assertEquals(0, transaction.topicPartitions.size());
    }
  }

  @Test
  void testQueryTransactionId() throws ExecutionException, InterruptedException {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer =
            Producer.builder().bootstrapServers(bootstrapServers()).buildTransactional()) {
      var handler = new TransactionHandler(admin);
      producer.sender().topic(topicName).value(new byte[1]).run().toCompletableFuture().get();

      var transaction =
          Assertions.assertInstanceOf(
              TransactionHandler.Transaction.class,
              handler.get(Optional.of(producer.transactionId().get()), Map.of()));

      Assertions.assertEquals(0, transaction.topicPartitions.size());
    }
  }

  @Test
  void queryNonexistentTransactionId() throws ExecutionException, InterruptedException {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer =
            Producer.builder().bootstrapServers(bootstrapServers()).buildTransactional()) {
      var handler = new TransactionHandler(admin);
      producer.sender().topic(topicName).value(new byte[1]).run().toCompletableFuture().get();

      Assertions.assertThrows(
          NoSuchElementException.class,
          () -> handler.get(Optional.of(Utils.randomString(10)), Map.of()));
    }
  }
}
