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

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.astraea.common.admin.TopicPartition;
import org.astraea.gui.Context;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;

public class TruncateRecordTab {

  private static final String TOPIC_NAME = "topic";
  private static final String PARTITION = "partition";
  private static final String OFFSET = "truncate to offset";

  public static Tab of(Context context) {

    var pane =
        PaneBuilder.of()
            .buttonName("TRUNCATE")
            .input(TOPIC_NAME, true, false)
            .input(PARTITION, false, true)
            .input(OFFSET, false, true)
            .buttonListener(
                (input, logger) -> {
                  var topic = input.nonEmptyTexts().get(TOPIC_NAME);
                  return context.submit(
                      admin ->
                          admin
                              .topicNames(false)
                              .thenCompose(
                                  names -> {
                                    if (!names.contains(topic))
                                      return CompletableFuture.failedFuture(
                                          new IllegalArgumentException(
                                              topic
                                                  + " is nonexistent. Also, you can't delete internal topic!"));
                                    var optionalPartitions =
                                        Optional.ofNullable(input.nonEmptyTexts().get(PARTITION))
                                            .map(
                                                s ->
                                                    Arrays.stream(s.split(","))
                                                        .map(Integer::parseInt)
                                                        .collect(Collectors.toSet()));
                                    var optionalOffset =
                                        Optional.ofNullable(input.nonEmptyTexts().get(OFFSET))
                                            .map(Long::parseLong);
                                    return admin
                                        .partitions(Set.of(topic))
                                        .thenCompose(
                                            allPartitions ->
                                                admin.deleteRecords(
                                                    allPartitions.stream()
                                                        .filter(
                                                            p ->
                                                                optionalPartitions
                                                                    .map(
                                                                        ps ->
                                                                            ps.contains(
                                                                                p.partition()))
                                                                    .orElse(true))
                                                        .collect(
                                                            Collectors.toMap(
                                                                p ->
                                                                    TopicPartition.of(
                                                                        p.topic(), p.partition()),
                                                                p ->
                                                                    optionalOffset
                                                                        .map(
                                                                            o ->
                                                                                Math.min(
                                                                                    o,
                                                                                    p
                                                                                        .latestOffset()))
                                                                        .orElse(
                                                                            p.latestOffset())))))
                                        .thenAccept(
                                            result ->
                                                result.forEach(
                                                    (tp, low) ->
                                                        logger.log(
                                                            tp
                                                                + " low watermark is changed to "
                                                                + low)));
                                  }));
                })
            .build();
    return Tab.of("truncate record", pane);
  }
}
