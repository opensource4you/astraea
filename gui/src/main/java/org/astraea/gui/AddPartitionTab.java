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
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import javafx.scene.control.TextField;

public class AddPartitionTab {

  public static Tab of(Context context) {
    var tab = new Tab("add partition");
    var topicField = new TextField();
    var partitionsField = Utils.onlyNumber(1);
    var applyButton = new Button("apply");
    var console = new ConsoleArea();
    applyButton.setOnAction(
        ignored -> {
          var name = topicField.getText();
          if (name.isEmpty()) {
            console.append("please enter topic name");
            return;
          }
          var partitions = partitionsField.getText();
          if (partitions.isEmpty()) {
            console.append("please enter number of partitions");
            return;
          }
          context.execute(
              admin ->
                  admin
                      .topicNames(true)
                      .thenCompose(
                          names -> {
                            if (!names.contains(name)) {
                              console.text(name + " is nonexistent");
                              return CompletableFuture.completedFuture(null);
                            }
                            return admin
                                .addPartitions(name, Integer.parseInt(partitions))
                                .whenComplete(
                                    (r, e) ->
                                        console.text(
                                            "succeed to increase partitions to " + partitions, e));
                          }));
        });
    tab.setContent(
        Utils.vbox(
            Utils.hbox(new Label("name:"), topicField),
            Utils.hbox(new Label("total partitions:"), partitionsField),
            applyButton,
            console));
    return tab;
  }
}
