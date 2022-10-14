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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.astraea.common.admin.TopicConfigs;
import org.astraea.gui.Context;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;

public class CreateTopicTab {

  private static final String TOPIC_NAME = "topic";
  private static final String NUMBER_OF_PARTITIONS = "number of partitions";
  private static final String NUMBER_OF_REPLICAS = "number of replicas";

  public static Tab of(Context context) {

    var pane =
        PaneBuilder.of()
            .buttonName("CREATE")
            .input(TOPIC_NAME, true, false)
            .input(NUMBER_OF_PARTITIONS, false, true)
            .input(NUMBER_OF_REPLICAS, false, true)
            .input(TopicConfigs.ALL_CONFIGS)
            .buttonListener(
                (input, logger) -> {
                  var allConfigs = new HashMap<>(input.nonEmptyTexts());
                  var name = allConfigs.remove(TOPIC_NAME);
                  return context
                      .admin()
                      .topicNames(true)
                      .thenCompose(
                          names -> {
                            if (names.contains(name))
                              return CompletableFuture.failedFuture(
                                  new IllegalArgumentException(name + " is already existent"));

                            return context
                                .admin()
                                .creator()
                                .topic(name)
                                .numberOfPartitions(
                                    Optional.ofNullable(allConfigs.remove(NUMBER_OF_PARTITIONS))
                                        .map(Integer::parseInt)
                                        .orElse(1))
                                .numberOfReplicas(
                                    Optional.ofNullable(allConfigs.remove(NUMBER_OF_REPLICAS))
                                        .map(Short::parseShort)
                                        .orElse((short) 1))
                                .configs(allConfigs)
                                .run()
                                .thenAccept(i -> logger.log("succeed to create topic:" + name));
                          });
                })
            .build();
    return Tab.of("create topic", pane);
  }
}
