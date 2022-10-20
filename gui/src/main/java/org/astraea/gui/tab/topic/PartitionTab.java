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
package org.astraea.gui.tab.topic;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.astraea.common.FutureUtils;
import org.astraea.common.MapUtils;
import org.astraea.common.admin.Partition;
import org.astraea.common.admin.Topic;
import org.astraea.common.admin.TopicPartition;
import org.astraea.gui.Context;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;
import org.astraea.gui.text.Label;
import org.astraea.gui.text.TextField;

public class PartitionTab {

  private static final String TOPIC_NAME_KEY = "topic";
  private static final String PARTITION_KEY = "partition";

  private static List<Map<String, Object>> basicResult(List<Partition> ps) {
    return ps.stream()
        .sorted(Comparator.comparing(Partition::topic).thenComparing(Partition::partition))
        .map(
            p -> {
              var result = new LinkedHashMap<String, Object>();
              result.put(TOPIC_NAME_KEY, p.topic());
              result.put(PARTITION_KEY, p.partition());
              p.leader().ifPresent(l -> result.put("leader", l.id()));
              result.put(
                  "replicas",
                  p.replicas().stream()
                      .map(n -> String.valueOf(n.id()))
                      .collect(Collectors.joining(",")));
              result.put(
                  "isr",
                  p.isr().stream()
                      .map(n -> String.valueOf(n.id()))
                      .collect(Collectors.joining(",")));
              result.put("earliest offset", p.earliestOffset());
              result.put("latest offset", p.latestOffset());
              p.maxTimestamp().ifPresent(t -> result.put("max timestamp", t));
              return result;
            })
        .collect(Collectors.toList());
  }

  static Tab tab(Context context) {
    var moveToKey = "move to brokers";
    var offsetKey = "truncate to offset";
    return Tab.of(
        "partition",
        PaneBuilder.of()
            .searchField("topic name")
            .tableViewAction(
                MapUtils.of(
                    Label.of(moveToKey),
                    TextField.builder().disable().build(),
                    Label.of(offsetKey),
                    TextField.builder().disable().build()),
                "ALTER",
                (items, inputs, logger) -> {
                  var partitions =
                      items.stream()
                          .flatMap(
                              item -> {
                                var topic = item.get(TOPIC_NAME_KEY);
                                var partition = item.get(PARTITION_KEY);
                                if (topic != null && partition != null)
                                  return Optional.of(
                                      TopicPartition.of(
                                          topic.toString(), Integer.parseInt(partition.toString())))
                                      .stream();
                                return Optional.<TopicPartition>empty().stream();
                              })
                          .collect(Collectors.toSet());
                  if (partitions.isEmpty()) {
                    logger.log("nothing to alert");
                    return CompletableFuture.completedStage(null);
                  }
                  var moveTo =
                      Optional.ofNullable(inputs.get(moveToKey))
                          .map(
                              s ->
                                  Arrays.stream(s.split(","))
                                      .map(Integer::parseInt)
                                      .collect(Collectors.toList()));
                  var offset = Optional.ofNullable(inputs.get(offsetKey)).map(Long::parseLong);
                  if (moveTo.isEmpty() && offset.isEmpty())
                    throw new IllegalArgumentException(
                        "Please define either \"move to\" or \"offset\"");

                  return context
                      .admin()
                      .topics(
                          partitions.stream()
                              .map(TopicPartition::topic)
                              .collect(Collectors.toSet()))
                      .thenCompose(
                          topics -> {
                            var internal =
                                topics.stream()
                                    .filter(Topic::internal)
                                    .map(Topic::name)
                                    .collect(Collectors.toSet());
                            if (!internal.isEmpty())
                              throw new IllegalArgumentException(
                                  "internal topics: " + internal + " can't be altered");
                            return FutureUtils.combine(
                                context
                                    .admin()
                                    .deleteRecords(
                                        offset
                                            .map(
                                                o ->
                                                    partitions.stream()
                                                        .collect(
                                                            Collectors.toMap(tp -> tp, tp -> o)))
                                            .orElse(Map.of())),
                                context
                                    .admin()
                                    .moveToBrokers(
                                        moveTo
                                            .map(
                                                bks ->
                                                    partitions.stream()
                                                        .collect(
                                                            Collectors.toMap(tp -> tp, tp -> bks)))
                                            .orElse(Map.of())),
                                (i, j) -> {
                                  logger.log("succeed to alter partitions: " + partitions);
                                  return null;
                                });
                          });
                })
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
                        .thenCompose(context.admin()::partitions)
                        .thenApply(PartitionTab::basicResult))
            .build());
  }
}
