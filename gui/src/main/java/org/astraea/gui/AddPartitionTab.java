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

import java.util.concurrent.CompletableFuture;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashSet;

public class AddPartitionTab {

  private static final String TOPIC_NAME = "topic";
  private static final String NUMBER_OF_PARTITIONS = "number of partitions";

  public static Tab of(Context context) {
    var tab = new Tab("add partition");
    tab.setContent(
        Utils.form(
            LinkedHashSet.of(TOPIC_NAME, NUMBER_OF_PARTITIONS),
            LinkedHashSet.of(),
            (result, console) -> {
              var name = result.get(TOPIC_NAME);
              if (name == null || name.isEmpty())
                return CompletableFuture.failedFuture(
                    new IllegalArgumentException("please enter topic name"));
              var partitions = result.get(NUMBER_OF_PARTITIONS);
              if (partitions == null || partitions.isEmpty())
                return CompletableFuture.failedFuture(
                    new IllegalArgumentException("please enter total number of partitions"));
              return context.submit(
                  admin ->
                      admin
                          .topicNames(true)
                          .thenCompose(
                              names -> {
                                if (!names.contains(name))
                                  return CompletableFuture.failedFuture(
                                      new IllegalArgumentException(name + " is nonexistent"));

                                return admin
                                    .addPartitions(name, Integer.parseInt(partitions))
                                    .thenApply(
                                        r -> "succeed to increase partitions to " + partitions);
                              }));
            },
            "EXECUTE"));
    return tab;
  }
}
