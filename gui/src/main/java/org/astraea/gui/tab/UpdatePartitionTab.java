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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.scene.layout.Pane;
import org.astraea.common.Utils;
import org.astraea.common.admin.Partition;
import org.astraea.common.admin.TopicPartition;
import org.astraea.gui.Context;
import org.astraea.gui.pane.BorderPane;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;

public class UpdatePartitionTab {

  private static final String PARTITIONS = "partitions";
  private static final String MOVE_TO = "move to";

  private static final String OFFSET = "truncate to";

  private static Pane pane(Context context, String topic, List<Partition> partitions) {
    return PaneBuilder.of()
        .buttonName("UPDATE")
        .input(PARTITIONS, true, false)
        .input(MOVE_TO, false, false)
        .input(OFFSET, false, true)
        .tableView(PartitionTab.result(partitions))
        .buttonAction(
            (input, logger) ->
                context.submit(
                    admin ->
                        // delay to fetch partitions
                        CompletableFuture.runAsync(() -> Utils.sleep(Duration.ofSeconds(2)))
                            .thenCompose(ignored -> admin.partitions(Set.of(topic)))
                            .thenApply(PartitionTab::result)))
        .buttonListener(
            (input, logger) -> {
              var topicPartitions =
                  Arrays.stream(input.nonEmptyTexts().get(PARTITIONS).split(","))
                      .map(Integer::parseInt)
                      .map(id -> TopicPartition.of(topic, id))
                      .collect(Collectors.toUnmodifiableSet());
              var offsetToDelete =
                  Optional.ofNullable(input.nonEmptyTexts().get(OFFSET))
                      .map(Long::parseLong)
                      .map(
                          offset ->
                              topicPartitions.stream()
                                  .collect(
                                      Collectors.toMap(Function.identity(), ignored -> offset)))
                      .orElse(Map.of());
              var moveTo =
                  Optional.ofNullable(input.nonEmptyTexts().get(MOVE_TO))
                      .map(
                          s ->
                              Arrays.stream(s.split(","))
                                  .map(Integer::parseInt)
                                  .collect(Collectors.toList()))
                      .map(
                          m ->
                              topicPartitions.stream()
                                  .collect(Collectors.toMap(Function.identity(), ignored -> m)))
                      .orElse(Map.of());
              if (offsetToDelete.isEmpty() && moveTo.isEmpty()) {
                logger.log("Please define either \"move to\" or \"offset\"");
                return CompletableFuture.completedFuture(null);
              }
              return context.submit(
                  admin ->
                      admin
                          .deleteRecords(offsetToDelete)
                          .thenCompose(ignored -> admin.moveToBrokers(moveTo))
                          .thenAccept(
                              ignored -> logger.log("succeed to update " + topicPartitions)));
            })
        .build();
  }

  public static Tab of(Context context) {

    return Tab.dynamic(
        "update partition",
        () ->
            context.submit(
                admin ->
                    admin
                        .topicNames(false)
                        .thenApply(
                            topics ->
                                BorderPane.dynamic(
                                    topics,
                                    topic ->
                                        admin
                                            .partitions(Set.of(topic))
                                            .thenApply(ps -> pane(context, topic, ps))))));
  }
}
