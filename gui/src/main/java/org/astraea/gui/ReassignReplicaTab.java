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
import java.util.function.BiConsumer;
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

    BiFunction<String, Console, SearchResult<Set<String>>> resultGenerator =
        (word, console) -> {
          var optionalAdmin = context.optionalAdmin();
          if (optionalAdmin.isEmpty()) return SearchResult.empty();
          var admin = optionalAdmin.get();
          var partitions =
              admin.partitions(
                  admin.topicNames().stream()
                      .filter(name -> word.isEmpty() || name.contains(word))
                      .collect(Collectors.toSet()));
          var selectedIds =
              partitions.stream()
                  .flatMap(p -> p.replicas().stream().map(NodeInfo::id))
                  .collect(Collectors.toSet());
          var allNodeIds = admin.brokerIds();
          removedIdBox.values(
              allNodeIds.stream().filter(selectedIds::contains).collect(Collectors.toSet()));
          addedIdBox.values(allNodeIds);
          var topics = partitions.stream().map(Partition::topic).collect(Collectors.toSet());
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
        };

    BiConsumer<SearchResult<Set<String>>, Console> resultExecutor =
        (result, console) -> {
          var optionalAdmin = context.optionalAdmin();
          if (optionalAdmin.isEmpty()) return;
          var admin = optionalAdmin.get();
          var removedId = removedIdBox.getValue();
          var addedId = addedIdBox.getValue();
          if (removedId == null && addedId == null) {
            console.append("please define either \"removed\" or \"added\"");
            return;
          }
          var replicas =
              admin.replicas(result.object()).stream()
                  .collect(Collectors.groupingBy(ReplicaInfo::topicPartition));
          replicas.forEach(
              (tp, rs) -> {
                var moveTo =
                    Stream.concat(
                            rs.stream()
                                .map(r -> r.nodeInfo().id())
                                .filter(id -> removedId == null || id != removedId.intValue()),
                            addedId == null ? Stream.of() : Stream.of(addedId))
                        .collect(Collectors.toCollection(LinkedHashSet::new));
                if (moveTo.isEmpty()) {
                  console.append("the new assignment of " + tp + " is empty. skip it");
                  return;
                }
                admin
                    .migrator()
                    .partition(tp.topic(), tp.partition())
                    .moveTo(new ArrayList<>(moveTo));
                console.append(
                    tp
                        + " is reassigned to ["
                        + moveTo.stream().map(String::valueOf).collect(Collectors.joining(","))
                        + "]");
              });
        };

    tab.setContent(
        Utils.vbox(
            Utils.searchToTable(
                resultGenerator,
                resultExecutor,
                List.of(new Label("removed:"), removedIdBox, new Label("added:"), addedIdBox))));
    return tab;
  }
}
