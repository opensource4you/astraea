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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import javafx.scene.control.Tab;
import org.astraea.common.Utils;
import org.astraea.common.admin.Partition;

public class MoveTopicTab {

  private static final String TOPIC_NAME = "topic";
  private static final String PARTITION_ID = "partition";
  private static final String MOVE_TO = "move to";

  public static Tab of(Context context) {
    var pane =
        PaneBuilder.of()
            .buttonName("MOVE")
            .input(TOPIC_NAME, true, false)
            .input(MOVE_TO, true, false)
            .input(PARTITION_ID, false, true)
            .buttonMessageAction(
                input -> {
                  var topic = input.texts().get(TOPIC_NAME);

                  var moveTo =
                      Arrays.stream(input.texts().get(MOVE_TO).split(","))
                          .map(Integer::parseInt)
                          .collect(Collectors.toList());
                  var partitions =
                      Optional.ofNullable(input.texts().get(PARTITION_ID))
                          .map(Integer::parseInt)
                          .map(partition -> CompletableFuture.completedStage(List.of(partition)))
                          .orElseGet(
                              () ->
                                  context.submit(
                                      admin ->
                                          admin
                                              .partitions(Set.of(topic))
                                              .thenApply(
                                                  ps ->
                                                      ps.stream()
                                                          .map(Partition::partition)
                                                          .collect(Collectors.toList()))));
                  return partitions.thenCompose(
                      ps ->
                          Utils.sequence(
                                  ps.stream()
                                      .map(
                                          p ->
                                              context.submit(
                                                  admin ->
                                                      admin
                                                          .migrator()
                                                          .partition(topic, p)
                                                          .moveTo(moveTo)))
                                      .map(CompletionStage::toCompletableFuture)
                                      .collect(Collectors.toList()))
                              .thenApply(
                                  ignored ->
                                      "succeed to move " + topic + "-" + ps + " to " + moveTo));
                })
            .build();

    var tab = new Tab("move topic");
    tab.setContent(pane);
    return tab;
  }
}
