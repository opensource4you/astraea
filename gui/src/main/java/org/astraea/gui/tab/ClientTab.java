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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.geometry.Side;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.ConsumerGroup;
import org.astraea.common.admin.ProducerState;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.Transaction;
import org.astraea.gui.Context;
import org.astraea.gui.pane.Input;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;
import org.astraea.gui.pane.TabPane;

public class ClientTab {
  private static List<Map<String, Object>> consumerResult(Stream<ConsumerGroup> cgs, Input input) {
    return cgs.flatMap(
            cg ->
                cg.assignment().entrySet().stream()
                    .flatMap(
                        entry ->
                            entry.getValue().stream()
                                .filter(
                                    tp ->
                                        input.matchSearch(tp.topic())
                                            || input.matchSearch(entry.getKey().groupId()))
                                .map(
                                    tp ->
                                        LinkedHashMap.<String, Object>of(
                                            "group",
                                            entry.getKey().groupId(),
                                            "coordinator",
                                            cg.coordinator().id(),
                                            "topic",
                                            tp.topic(),
                                            "partition",
                                            tp.partition(),
                                            "offset",
                                            Optional.ofNullable(cg.consumeProgress().get(tp))
                                                .map(String::valueOf)
                                                .orElse("unknown"),
                                            "client host",
                                            entry.getKey().host(),
                                            "client id",
                                            entry.getKey().clientId(),
                                            "member id",
                                            entry.getKey().memberId(),
                                            "instance id",
                                            entry.getKey().groupInstanceId().orElse("")))))
        .collect(Collectors.toList());
  }

  public static Tab consumerTab(Context context) {
    return Tab.of(
        "consumer",
        PaneBuilder.of()
            .searchField("group id or topic name")
            .buttonAction(
                (input, logger) ->
                    context
                        .admin()
                        .consumerGroupIds()
                        .thenCompose(context.admin()::consumerGroups)
                        .thenApply(cgs -> consumerResult(cgs.stream(), input)))
            .build());
  }

  private static List<Map<String, Object>> transactionResult(Stream<Transaction> transactions) {
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

  public static Tab transactionTab(Context context) {
    return Tab.of(
        "transaction",
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
                        .thenApply(ClientTab::transactionResult))
            .build());
  }

  private static List<Map<String, Object>> producerResult(Stream<ProducerState> states) {
    return states
        .map(
            state ->
                LinkedHashMap.<String, Object>of(
                    "topic",
                    state.topic(),
                    "partition",
                    state.partition(),
                    "producer id",
                    state.producerId(),
                    "producer epoch",
                    state.producerEpoch(),
                    "last sequence",
                    state.lastSequence(),
                    "last timestamp",
                    state.lastTimestamp()))
        .collect(Collectors.toList());
  }

  public static Tab producerTab(Context context) {
    return Tab.of(
        "producer",
        PaneBuilder.of()
            .searchField("topic name")
            .buttonAction(
                (input, logger) ->
                    context
                        .admin()
                        .topicNames(true)
                        .thenApply(
                            names ->
                                names.stream()
                                    .filter(input::matchSearch)
                                    .collect(Collectors.toSet()))
                        .thenCompose(context.admin()::topicPartitions)
                        .thenCompose(context.admin()::producerStates)
                        .thenApply(
                            ps ->
                                ps.stream()
                                    .sorted(
                                        Comparator.comparing(ProducerState::topic)
                                            .thenComparing(ProducerState::partition)))
                        .thenApply(ClientTab::producerResult))
            .build());
  }

  public static Tab of(Context context) {
    return Tab.of(
        "client",
        TabPane.of(
            Side.TOP,
            List.of(consumerTab(context), producerTab(context), transactionTab(context))));
  }
}
