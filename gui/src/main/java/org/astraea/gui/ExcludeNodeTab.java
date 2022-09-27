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
import javafx.scene.control.Tab;
import javafx.scene.control.TextField;
import javafx.scene.layout.VBox;
import org.astraea.common.admin.ReplicaInfo;

public class ExcludeNodeTab {

  public static Tab of(Context context) {
    var tab = new Tab("exclude node");
    var pane = new VBox(4);
    pane.setPadding(new Insets(5));

    var selectedTopics = new AtomicReference<Set<String>>();
    var console = new Console("");
    var search = new TextField("enter to search");
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
                                      selectedTopics.set(names);
                                      console.text(String.join(",", names));
                                    }
                                  }));
            }));
    var excludedBrokerIdBox = new IntegerBox((short) 1);
    var excludeButton = new Button("exclude");
    pane.getChildren().setAll(search, excludedBrokerIdBox, console, excludeButton);
    excludeButton.setOnAction(
        ignored -> {
          var topics = selectedTopics.get();
          if (topics == null || topics.isEmpty()) return;
          var numberOfReplicas = excludedBrokerIdBox.getValue();
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
                                        console.text("remove " + tp + " from " + excludedBrokerId);
                                      }
                                    });
                                console.text("done!!!");
                              })
                          .whenComplete((none, e) -> console.text(e)));
        });
    tab.setContent(pane);
    tab.setOnSelectionChanged(
        ignored -> {
          if (!tab.isSelected()) return;
          context
              .optionalAdmin()
              .ifPresent(
                  admin ->
                      CompletableFuture.supplyAsync(admin::brokerIds)
                          .whenComplete((ids, e) -> excludedBrokerIdBox.values(ids)));
        });
    return tab;
  }
}
