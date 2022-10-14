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
package org.astraea.gui.tab;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.Transaction;
import org.astraea.gui.Context;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;

public class TransactionTab {

  private static List<Map<String, Object>> result(Stream<Transaction> transactions) {
    return transactions
        .map(
            transaction ->
                LinkedHashMap.<String, Object>of(
                    "transaction id", transaction.transactionId(),
                    "coordinator id", transaction.coordinatorId(),
                    "state", transaction.state().alias(),
                    "producer id", transaction.producerId(),
                    "producer epoch", transaction.producerEpoch(),
                    "timeout", transaction.transactionTimeoutMs(),
                    "partitions",
                        transaction.topicPartitions().stream()
                            .map(TopicPartition::toString)
                            .collect(Collectors.joining(","))))
        .collect(Collectors.toUnmodifiableList());
  }

  public static Tab of(Context context) {
    var pane =
        PaneBuilder.of()
            .searchField("topic name or transaction id")
            .buttonAction(
                (input, logger) ->
                    context
                        .admin()
                        .transactionIds()
                        .thenCompose(context.admin()::transactions)
                        .thenApply(
                            ts ->
                                ts.stream()
                                    .filter(
                                        transaction ->
                                            input.matchSearch(transaction.transactionId())
                                                || transaction.topicPartitions().stream()
                                                    .anyMatch(tp -> input.matchSearch(tp.topic()))))
                        .thenApply(TransactionTab::result))
            .build();
    return Tab.of("transaction", pane);
  }
}
