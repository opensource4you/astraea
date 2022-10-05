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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Partition;
import org.astraea.common.admin.ReplicaInfo;

public class ReassignReplicaTab {

  public static Tab of(Context context) {
    var tab = new Tab("reassign replica");
    var removedIdBox = new IntegerBox();
    var addedIdBox = new IntegerBox();

    BiFunction<String, Console, CompletionStage<SearchResult<Set<String>>>> resultGenerator =
        (word, console) ->
            context.submit(
                admin ->
                    admin
                        .topicNames(true)
                        .thenApply(
                            names ->
                                names.stream()
                                    .filter(name -> Utils.contains(name, word))
                                    .collect(Collectors.toSet()))
                        .thenCompose(admin::partitions)
                        .thenCombine(
                            admin.nodeInfos(),
                            (partitions, nodeInfos) -> {
                              var selectedIds =
                                  partitions.stream()
                                      .flatMap(p -> p.replicas().stream().map(NodeInfo::id))
                                      .collect(Collectors.toSet());
                              removedIdBox.values(
                                  nodeInfos.stream()
                                      .map(NodeInfo::id)
                                      .filter(selectedIds::contains)
                                      .collect(Collectors.toSet()));
                              addedIdBox.values(
                                  nodeInfos.stream().map(NodeInfo::id).collect(Collectors.toSet()));

                              var topics =
                                  partitions.stream()
                                      .map(Partition::topic)
                                      .collect(Collectors.toSet());
                              return SearchResult.of(
                                  partitions.stream()
                                      .map(
                                          p ->
                                              LinkedHashMap.<String, Object>of(
                                                  "topic",
                                                  p.topic(),
                                                  "partition",
                                                  p.partition(),
                                                  "brokers",
                                                  p.replicas().stream()
                                                      .map(n -> String.valueOf(n.id()))
                                                      .collect(Collectors.joining(","))))
                                      .collect(Collectors.toList()),
                                  topics);
                            }));

    BiFunction<SearchResult<Set<String>>, Console, CompletionStage<List<Void>>> resultExecutor =
        (result, console) ->
            context.submit(
                admin -> {
                  var removedId = removedIdBox.getValue();
                  var addedId = addedIdBox.getValue();
                  if (removedId == null && addedId == null) {
                    console.append("please define either \"removed\" or \"added\"");
                    return CompletableFuture.<List<Void>>completedFuture(null);
                  }
                  return admin
                      .replicas(result.object())
                      .thenApply(
                          replicas ->
                              replicas.stream()
                                  .collect(Collectors.groupingBy(ReplicaInfo::topicPartition)))
                      .thenCompose(
                          replicas ->
                              org.astraea.common.Utils.sequence(
                                  replicas.entrySet().stream()
                                      .map(
                                          entry -> {
                                            var tp = entry.getKey();
                                            var rs = entry.getValue();
                                            var moveTo =
                                                Stream.concat(
                                                        rs.stream()
                                                            .map(r -> r.nodeInfo().id())
                                                            .filter(
                                                                id ->
                                                                    removedId == null
                                                                        || id
                                                                            != removedId
                                                                                .intValue()),
                                                        addedId == null
                                                            ? Stream.of()
                                                            : Stream.of(addedId))
                                                    .collect(
                                                        Collectors.toCollection(
                                                            LinkedHashSet::new));
                                            if (moveTo.isEmpty()) {
                                              console.append(
                                                  "the new assignment of "
                                                      + tp
                                                      + " is empty. skip it");
                                              return CompletableFuture.<Void>completedFuture(null);
                                            }
                                            return admin
                                                .migrator()
                                                .partition(tp.topic(), tp.partition())
                                                .moveTo(new ArrayList<>(moveTo))
                                                .whenComplete(
                                                    (r, e) ->
                                                        console.append(
                                                            tp
                                                                + " is reassigned to ["
                                                                + moveTo.stream()
                                                                    .map(String::valueOf)
                                                                    .collect(
                                                                        Collectors.joining(","))
                                                                + "]"))
                                                .toCompletableFuture();
                                          })
                                      .collect(Collectors.toList())));
                });

    tab.setContent(
        Utils.vbox(
            Utils.searchToTable(
                resultGenerator,
                resultExecutor,
                List.of(new Label("removed:"), removedIdBox, new Label("added:"), addedIdBox))));
    return tab;
  }
}
