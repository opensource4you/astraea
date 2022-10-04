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
package org.astraea.gui;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.Transaction;

public class TransactionTab {

  private static List<LinkedHashMap<String, Object>> result(Stream<Transaction> transactions) {
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
        Utils.searchToTable(
            (word, console) ->
                context.submit(
                    admin ->
                        result(
                            admin.transactions(admin.transactionIds()).stream()
                                .filter(
                                    transaction ->
                                        word.isEmpty()
                                            || transaction.transactionId().contains(word)
                                            || transaction.topicPartitions().stream()
                                                .anyMatch(tp -> tp.topic().contains(word))))));
    var tab = new Tab("transaction");
    tab.setContent(pane);
    return tab;
  }
}
