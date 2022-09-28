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

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import javafx.scene.control.TextField;
import javafx.scene.layout.VBox;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.ReplicaInfo;

public class ExcludeNodeTab {

  public static Tab of(Context context) {
    var tab = new Tab("exclude node");

    var selectedTopics = new AtomicReference<Set<String>>();
    var searchConsole = new Console("");
    var excludedBrokerIdBox = new IntegerBox((short) 1);
    var search = new TextField("");
    search
        .textProperty()
        .addListener(
            ((observable, oldValue, newValue) -> {
              if (newValue == null) return;
              if (newValue.equals(oldValue)) return;
              context
                  .optionalAdmin()
                  .ifPresent(
                      admin ->
                          CompletableFuture.supplyAsync(
                                  () ->
                                      admin.topicNames().stream()
                                          .filter(
                                              name -> newValue.isEmpty() || name.contains(newValue))
                                          .collect(Collectors.toSet()))
                              .whenComplete(
                                  (names, e) -> {
                                    if (names != null) {
                                      var nodeIds =
                                          admin.nodes().stream()
                                              .filter(
                                                  n ->
                                                      n.folders().stream()
                                                          .anyMatch(
                                                              f ->
                                                                  f
                                                                      .partitionSizes()
                                                                      .keySet()
                                                                      .stream()
                                                                      .anyMatch(
                                                                          tp ->
                                                                              names.contains(
                                                                                  tp.topic()))))
                                              .map(NodeInfo::id)
                                              .collect(Collectors.toSet());
                                      excludedBrokerIdBox.values(nodeIds);
                                      selectedTopics.set(names);
                                      searchConsole.text(
                                          names.size()
                                              + " topics are selected: "
                                              + String.join(", ", names));
                                    }
                                  }));
            }));
    var excludeButtonConsole = new Console("");
    var excludeButton = new Button("exclude");
    var nodes =
        List.of(
            new Label("search for topics:"),
            search,
            searchConsole,
            new Label("please select a broker:"),
            excludedBrokerIdBox,
            excludeButtonConsole,
            excludeButton);
    var pane = new VBox(nodes.size());
    pane.setPadding(new Insets(15));
    pane.getChildren().setAll(nodes);
    excludeButton.setOnAction(
        ignored -> {
          var topics = selectedTopics.get();
          if (topics == null || topics.isEmpty()) return;
          var numberOfReplicas = excludedBrokerIdBox.getValue();
          if (numberOfReplicas == null) return;
          context
              .optionalAdmin()
              .ifPresent(
                  admin ->
                      CompletableFuture.runAsync(
                              () -> {
                                var allBrokerIds = admin.brokerIds();
                                int excludedBrokerId = excludedBrokerIdBox.getValue();
                                var replicas =
                                    admin.newReplicas(topics).stream()
                                        .collect(
                                            Collectors.groupingBy(ReplicaInfo::topicPartition));
                                replicas.forEach(
                                    (tp, rs) -> {
                                      var remainingBrokerIds =
                                          rs.stream()
                                              .map(r -> r.nodeInfo().id())
                                              .filter(id -> id != excludedBrokerId)
                                              .collect(Collectors.toList());
                                      if (remainingBrokerIds.size() != rs.size()) {
                                        // if the partition has only one excluded replica,
                                        // we have to move it to another node.
                                        var moveTo =
                                            remainingBrokerIds.isEmpty()
                                                ? allBrokerIds.stream()
                                                    .filter(id -> id != excludedBrokerId)
                                                    .findAny()
                                                    .map(List::of)
                                                    .orElseThrow(
                                                        () ->
                                                            new IllegalArgumentException(
                                                                "there is no enough brokers to exclude "
                                                                    + excludedBrokerId))
                                                : remainingBrokerIds;
                                        admin
                                            .migrator()
                                            .partition(tp.topic(), tp.partition())
                                            .moveTo(moveTo);
                                        excludeButtonConsole.text(
                                            "remove " + tp + " from " + excludedBrokerId);
                                      }
                                    });
                                excludeButtonConsole.text("done!!!");
                              })
                          .whenComplete((none, e) -> excludeButtonConsole.text(e)));
        });
    tab.setContent(pane);
    return tab;
  }
}
