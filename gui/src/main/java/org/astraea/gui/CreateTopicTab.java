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
import javafx.geometry.HPos;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;

public class CreateTopicTab {

  public static Tab of(Context context) {
    var tab = new Tab("create topic");
    var pane = new GridPane();
    pane.setPadding(new Insets(5));
    pane.setVgap(4);
    pane.setHgap(2);

    // topic
    var topicLabel = new Label("topic name:");
    GridPane.setHalignment(topicLabel, HPos.RIGHT);
    pane.add(topicLabel, 0, 0);
    var topicField = new TextField();
    pane.add(topicField, 1, 0);

    // partitions
    var partitionsLabel = new Label("number of partitions:");
    GridPane.setHalignment(partitionsLabel, HPos.RIGHT);
    pane.add(partitionsLabel, 0, 1);
    var partitionsField = Utils.onlyNumber(1);
    pane.add(partitionsField, 1, 1);

    // replicas
    var replicasLabel = new Label("number of replicas:");
    GridPane.setHalignment(replicasLabel, HPos.RIGHT);
    pane.add(replicasLabel, 0, 2);
    var replicasField = new ShortBox((short) 1);
    GridPane.setHalignment(replicasField, HPos.LEFT);
    pane.add(replicasField, 1, 2);

    var btn = new Button("create");
    var console = new Console("");
    pane.add(btn, 0, 3);
    pane.add(console, 1, 3);
    btn.setOnAction(
        ignored -> {
          var name = topicField.getText();
          if (name.isEmpty()) {
            console.text("please enter topic name");
            return;
          }
          context
              .optionalAdmin()
              .ifPresent(
                  admin ->
                      CompletableFuture.supplyAsync(
                              () -> {
                                if (admin.topicNames().contains(name)) return name + " is existent";
                                admin
                                    .creator()
                                    .topic(name)
                                    .numberOfPartitions(Integer.parseInt(partitionsField.getText()))
                                    .numberOfReplicas(replicasField.getValue())
                                    .create();
                                return "succeed to create " + name;
                              })
                          .whenComplete(console::text));
        });
    tab.setContent(pane);
    tab.setOnSelectionChanged(
        ignored -> {
          if (!tab.isSelected()) return;
          context
              .optionalAdmin()
              .ifPresent(
                  admin ->
                      CompletableFuture.supplyAsync(() -> admin.brokerIds().size())
                          .whenComplete(
                              (size, e) -> replicasField.range((short) 0, size.shortValue())));
        });
    return tab;
  }
}
