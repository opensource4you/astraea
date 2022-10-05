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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import javafx.scene.control.TextField;

public class CreateTopicTab {

  public static Tab of(Context context) {
    var tab = new Tab("create topic");
    var topicField = new TextField();
    var partitionsField = Utils.onlyNumber(1);
    var replicasField = new IntegerBox(1);

    var executeButton = new Button("create");
    var console = new ConsoleArea();
    executeButton.setOnAction(
        ignored -> {
          var name = topicField.getText();
          if (name.isEmpty()) {
            console.append("please enter topic name");
            return;
          }
          context.execute(
              admin ->
                  admin
                      .topicNames(true)
                      .thenCompose(
                          names -> {
                            if (names.contains(name))
                              return CompletableFuture.completedFuture(name + " is existent");
                            return admin
                                .creator()
                                .topic(name)
                                .numberOfPartitions(Integer.parseInt(partitionsField.getText()))
                                .numberOfReplicas(replicasField.getValue().shortValue())
                                .run()
                                .thenApply(nonexistent -> "succeed to create " + name);
                          })
                      .whenComplete(console::text));
        });
    tab.setContent(
        Utils.vbox(
            Utils.hbox(new Label("name:"), topicField),
            Utils.hbox(new Label("partitions:"), partitionsField),
            Utils.hbox(new Label("replicas"), replicasField),
            executeButton,
            console));
    tab.setOnSelectionChanged(
        ignored -> {
          if (!tab.isSelected()) return;
          context.execute(
              admin ->
                  admin
                      .brokers()
                      .whenComplete(
                          (brokers, e) ->
                              replicasField.values(
                                  IntStream.range(0, brokers.size())
                                      .mapToObj(i -> i + 1)
                                      .collect(Collectors.toList()),
                                  0)));
        });
    return tab;
  }
}
