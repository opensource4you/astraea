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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.geometry.Side;
import org.astraea.common.FutureUtils;
import org.astraea.common.MapUtils;
import org.astraea.common.admin.ConsumerGroup;
import org.astraea.common.admin.Partition;
import org.astraea.common.admin.ProducerState;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.Transaction;
import org.astraea.gui.Context;
import org.astraea.gui.pane.Input;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;
import org.astraea.gui.pane.TabPane;

public class ClientTab {

  private static final String ACTIVE_KEY = "active";

  private static List<Map<String, Object>> consumerResult(
      List<ConsumerGroup> cgs, List<Partition> partitions, Input input) {
    var pts = partitions.stream().collect(Collectors.groupingBy(Partition::topicPartition));
    return cgs.stream()
        .filter(
            cg ->
                !input.multiSelectedRadios(List.<String>of()).contains(ACTIVE_KEY)
                    || !cg.assignment().isEmpty())
        .flatMap(
            cg ->
                Stream.concat(
                        cg.consumeProgress().keySet().stream(),
                        cg.assignment().values().stream().flatMap(Collection::stream))
                    .map(
                        tp -> {
                          var result = new LinkedHashMap<String, Object>();
                          result.put("group", cg.groupId());
                          result.put("coordinator", cg.coordinator().id());
                          result.put("topic", tp.topic());
                          result.put("partition", tp.partition());
                          Optional.ofNullable(cg.consumeProgress().get(tp))
                              .ifPresent(offset -> result.put("offset", offset));
                          result.put(
                              "lag",
                              pts.get(tp).get(0).latestOffset()
                                  - Optional.ofNullable(cg.consumeProgress().get(tp)).orElse(0L));
                          cg.assignment().entrySet().stream()
                              .filter(e -> e.getValue().contains(tp))
                              .findFirst()
                              .map(Map.Entry::getKey)
                              .ifPresent(
                                  member -> {
                                    result.put("client host", member.host());
                                    result.put("client id", member.clientId());
                                    result.put("member id", member.memberId());
                                    member
                                        .groupInstanceId()
                                        .ifPresent(
                                            instanceId -> result.put("instance id", instanceId));
                                  });
                          return result;
                        }))
        .collect(Collectors.toList());
  }

  public static Tab consumerTab(Context context) {
    return Tab.of(
        "consumer",
        PaneBuilder.of()
            .multiRadioButtons(List.of(ACTIVE_KEY))
            .buttonAction(
                (input, logger) ->
                    FutureUtils.combine(
                        context
                            .admin()
                            .consumerGroupIds()
                            .thenCompose(context.admin()::consumerGroups),
                        context
                            .admin()
                            .topicNames(true)
                            .thenCompose(names -> context.admin().partitions(names)),
                        (cgs, partitions) -> consumerResult(cgs, partitions, input)))
            .build());
  }

  private static List<Map<String, Object>> transactionResult(List<Transaction> transactions) {
    return transactions.stream()
        .map(
            transaction ->
                MapUtils.<String, Object>of(
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
            .buttonAction(
                (input, logger) ->
                    context
                        .admin()
                        .transactionIds()
                        .thenCompose(context.admin()::transactions)
                        .thenApply(ClientTab::transactionResult))
            .build());
  }

  private static List<Map<String, Object>> producerResult(Stream<ProducerState> states) {
    return states
        .map(
            state ->
                MapUtils.<String, Object>of(
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
                    LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(state.lastTimestamp()), ZoneId.systemDefault())))
        .collect(Collectors.toList());
  }

  public static Tab producerTab(Context context) {
    return Tab.of(
        "producer",
        PaneBuilder.of()
            .buttonAction(
                (input, logger) ->
                    context
                        .admin()
                        .topicNames(true)
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
