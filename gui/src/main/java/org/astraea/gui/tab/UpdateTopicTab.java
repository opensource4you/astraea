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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javafx.scene.layout.Pane;
import org.astraea.common.Utils;
import org.astraea.common.admin.Topic;
import org.astraea.common.admin.TopicConfigs;
import org.astraea.gui.Context;
import org.astraea.gui.pane.BorderPane;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;

public class UpdateTopicTab {

  private static final String NUMBER_OF_PARTITIONS = "number of partitions";

  private static Pane pane(Context context, Topic topic) {
    return PaneBuilder.of()
        .buttonName("UPDATE")
        .input(NUMBER_OF_PARTITIONS, false, true, String.valueOf(topic.topicPartitions().size()))
        .input(
            TopicConfigs.DYNAMICAL_CONFIGS.stream()
                .collect(Collectors.toMap(k -> k, k -> topic.config().value(k).orElse(""))))
        .buttonListener(
            (input, logger) -> {
              var allConfigs = new HashMap<>(input.nonEmptyTexts());
              var partitions = Integer.parseInt(allConfigs.remove(NUMBER_OF_PARTITIONS));
              return context
                  .admin()
                  .setConfigs(topic.name(), allConfigs)
                  .thenCompose(
                      ignored -> context.admin().unsetConfigs(topic.name(), input.emptyValueKeys()))
                  .thenCompose(
                      ignored ->
                          partitions == topic.topicPartitions().size()
                              ? CompletableFuture.completedFuture(null)
                              : context.admin().addPartitions(topic.name(), partitions))
                  .thenAccept(ignored -> logger.log("succeed to update " + topic.name()));
            })
        .build();
  }

  public static Tab of(Context context) {
    return Tab.dynamic(
        "update topic",
        () ->
            context
                .admin()
                .topicNames(false)
                .thenCompose(context.admin()::topics)
                .thenApply(
                    topics ->
                        BorderPane.selectableTop(
                            topics.stream()
                                .collect(
                                    Utils.toSortedMap(
                                        Topic::name, topic -> pane(context, topic))))));
  }
}
