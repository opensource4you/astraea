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
package org.astraea.gui.tab.health;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.geometry.Side;
import javafx.scene.Node;
import org.astraea.common.FutureUtils;
import org.astraea.common.MapUtils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Topic;
import org.astraea.common.admin.TopicChecker;
import org.astraea.common.admin.TopicConfigs;
import org.astraea.gui.Context;
import org.astraea.gui.pane.FirstPart;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Slide;

public class HealthNode {

  public static Node healthNode(Context context) {
    var firstPart =
        FirstPart.builder()
            .clickName("CHECK")
            .tablesRefresher(
                (argument, logger) ->
                    FutureUtils.combine(
                        badTopics(context.admin()),
                        unavailablePartitions(context.admin()),
                        (topics, partitions) -> {
                          var result = new LinkedHashMap<String, List<Map<String, Object>>>();
                          result.put("topic", topics);
                          result.put("partition", partitions);
                          return result;
                        }))
            .build();
    return PaneBuilder.of().firstPart(firstPart).build();
  }

  static CompletionStage<List<Map<String, Object>>> badTopics(Admin admin) {
    return FutureUtils.combine(
        admin.topicNames(List.of(TopicChecker.NO_DATA)),
        admin.topicNames(List.of(TopicChecker.NO_CONSUMER_GROUP)),
        admin.topicNames(
            List.of(TopicChecker.noWriteAfter(Duration.ofHours(1), Duration.ofSeconds(1)))),
        admin.topicNames(List.of(TopicChecker.skewPartition(0.5))),
        (noDataTopics, noConsumerTopics, noWriteTopics, skewTopics) ->
            Stream.of(
                    noDataTopics.stream(),
                    noConsumerTopics.stream(),
                    noWriteTopics.stream(),
                    skewTopics.stream())
                .flatMap(s -> s)
                .distinct()
                .map(
                    name -> {
                      var r = new LinkedHashMap<String, Object>();
                      r.put("topic", name);
                      r.put("empty", noDataTopics.contains(name));
                      r.put("no consumer group", noConsumerTopics.contains(name));
                      r.put("no write (1 hour)", noWriteTopics.contains(name));
                      r.put("unbalanced", skewTopics.contains(name));
                      return r;
                    })
                .collect(Collectors.toList()));
  }

  static CompletionStage<List<Map<String, Object>>> unavailablePartitions(Admin admin) {
    return admin
        .topicNames(true)
        .thenCompose(
            names ->
                FutureUtils.combine(
                    admin.topics(names),
                    admin.partitions(names),
                    (topics, partitions) -> {
                      var minInSync =
                          topics.stream()
                              .collect(
                                  Collectors.toMap(
                                      Topic::name,
                                      t ->
                                          t.config()
                                              .value(TopicConfigs.MIN_IN_SYNC_REPLICAS_CONFIG)
                                              .map(Integer::parseInt)
                                              .orElse(1)));

                      return partitions.stream()
                          .filter(
                              p ->
                                  p.isr().size() < minInSync.getOrDefault(p.topic(), 1)
                                      || p.leaderId().isEmpty())
                          .map(
                              p -> {
                                var r = new LinkedHashMap<String, Object>();
                                r.put("topic", p.topic());
                                r.put("partition", p.partition());
                                r.put("leader", p.leaderId().map(String::valueOf).orElse("null"));
                                r.put(
                                    "in-sync replicas",
                                    p.isr().stream()
                                        .map(n -> String.valueOf(n.id()))
                                        .collect(Collectors.joining(",")));
                                r.put(
                                    TopicConfigs.MIN_IN_SYNC_REPLICAS_CONFIG,
                                    minInSync.getOrDefault(p.topic(), 1));
                                r.put("readable", p.leaderId().isPresent());
                                r.put(
                                    "writable",
                                    p.leaderId().isPresent()
                                        && p.isr().size() >= minInSync.getOrDefault(p.topic(), 1));
                                return (Map<String, Object>) r;
                              })
                          .collect(Collectors.toList());
                    }));
  }

  public static Node of(Context context) {
    return Slide.of(
            Side.TOP,
            MapUtils.of("basic", healthNode(context), "balancer", BalancerNode.of(context)))
        .node();
  }
}
