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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.scene.Node;
import javafx.scene.layout.Pane;
import org.astraea.common.admin.Topic;
import org.astraea.common.admin.TopicConfigs;
import org.astraea.gui.Context;
import org.astraea.gui.pane.BorderPane;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;
import org.astraea.gui.text.TextField;

public class UpdateTopicTab {

  private static final String NUMBER_OF_PARTITIONS = "number of partitions";

  private static Pane pane(Context context, Topic topic) {
    return PaneBuilder.of()
        .buttonName("UPDATE")
        .input(NUMBER_OF_PARTITIONS, false, true)
        .input(TopicConfigs.DYNAMICAL_CONFIGS)
        .inputInitializer(
            () ->
                CompletableFuture.completedFuture(
                    Stream.concat(
                            topic.config().raw().entrySet().stream(),
                            Stream.of(
                                Map.entry(
                                    NUMBER_OF_PARTITIONS,
                                    String.valueOf(topic.topicPartitions().size()))))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))))
        .buttonListener(
            (input, logger) -> {
              var allConfigs = new HashMap<>(input.nonEmptyTexts());
              var partitions = Integer.parseInt(allConfigs.remove(NUMBER_OF_PARTITIONS));
              return context.submit(
                  admin ->
                      admin
                          .setConfigs(topic.name(), allConfigs)
                          .thenCompose(
                              ignored -> admin.unsetConfigs(topic.name(), input.emptyValueKeys()))
                          .thenCompose(
                              ignored ->
                                  partitions == topic.topicPartitions().size()
                                      ? CompletableFuture.completedFuture(null)
                                      : admin.addPartitions(topic.name(), partitions))
                          .thenAccept(ignored -> logger.log("succeed to update " + topic.name())));
            })
        .build();
  }

  public static Tab of(Context context) {
    return Tab.dynamical(
        "update topic",
        () ->
            context
                .submit(admin -> admin.topicNames(false).thenCompose(admin::topics))
                .thenApply(
                    topics ->
                        BorderPane.dynamic(
                            topics.stream()
                                .map(Topic::name)
                                .collect(Collectors.toUnmodifiableSet()),
                            topic ->
                                CompletableFuture.completedFuture(
                                    topics.stream()
                                        .filter(t -> t.name().equals(topic))
                                        .findFirst()
                                        .map(t -> (Node) pane(context, t))
                                        .orElseGet(
                                            () ->
                                                TextField.of(
                                                    "selected topic: "
                                                        + topic
                                                        + " is deleted"))))));
  }
}
