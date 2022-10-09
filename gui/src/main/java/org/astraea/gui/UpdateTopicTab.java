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

import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javafx.scene.control.Tab;
import org.astraea.common.admin.TopicConfigs;

public class UpdateTopicTab {

  private static final String TOPIC_NAME = "topic";
  private static final String NUMBER_OF_PARTITIONS = "number of partitions";

  public static Tab of(Context context) {

    var pane =
        PaneBuilder.of()
            .buttonName("UPDATE")
            .input(TOPIC_NAME, true, false)
            .input(NUMBER_OF_PARTITIONS, false, true)
            .input(TopicConfigs.DYNAMICAL_CONFIGS)
            .buttonMessageAction(
                input -> {
                  var allConfigs = new HashMap<>(input.texts());
                  var name = allConfigs.remove(TOPIC_NAME);
                  var partitions = allConfigs.remove(NUMBER_OF_PARTITIONS);
                  return context.submit(
                      admin ->
                          admin
                              .topicNames(true)
                              .thenCompose(
                                  names -> {
                                    if (!names.contains(name))
                                      return CompletableFuture.failedFuture(
                                          new IllegalArgumentException(name + " is nonexistent"));

                                    return Optional.ofNullable(partitions)
                                        .map(Integer::parseInt)
                                        .map(ps -> admin.addPartitions(name, ps))
                                        .orElse(CompletableFuture.completedFuture(null))
                                        .thenCompose(
                                            ignored -> admin.updateConfig(name, allConfigs))
                                        .thenApply(
                                            ignored -> "succeed to update configs of " + name);
                                  }));
                })
            .build();
    var tab = new Tab("update topic");
    tab.setContent(pane);
    return tab;
  }
}
