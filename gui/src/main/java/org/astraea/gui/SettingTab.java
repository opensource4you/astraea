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
import java.util.concurrent.CompletableFuture;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import javafx.scene.control.TextField;
import javafx.scene.layout.VBox;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;

public class SettingTab {

  public static Tab of(Context context) {
    var tab = new Tab("bootstrap servers");

    var bootstrapField = new TextField("");
    var console = new Console("");
    var checkButton = new Button("check");
    var ns =
        List.of(new Label("enter the bootstrap servers:"), bootstrapField, console, checkButton);
    var pane = new VBox(ns.size());
    pane.setPadding(new Insets(15));
    pane.getChildren().setAll(ns);
    tab.setContent(pane);
    checkButton.setOnAction(
        ignored -> {
          var bootstrapServers = bootstrapField.getText();
          if (!bootstrapServers.isEmpty())
            CompletableFuture.supplyAsync(
                    () -> {
                      var previous = context.replace(Admin.of(bootstrapServers));
                      previous.ifPresent(admin -> Utils.swallowException(admin::close));
                      return "succeed to connect to " + bootstrapServers;
                    })
                .whenComplete(console::text);
        });
    return tab;
  }
}
