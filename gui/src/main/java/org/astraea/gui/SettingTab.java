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
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import javafx.scene.control.TextField;
import org.astraea.common.admin.Admin;

public class SettingTab {

  public static Tab of(Context context) {
    var tab = new Tab("bootstrap servers");

    var bootstrapField = new TextField("");
    var console = new Console("");
    var checkButton = new Button("check");
    tab.setContent(
        Utils.vbox(
            Utils.hbox(new Label("bootstrap servers:"), bootstrapField),
            Utils.hbox(Pos.TOP_LEFT, checkButton, console)));
    checkButton.setOnAction(
        ignored -> {
          var bootstrapServers = bootstrapField.getText();
          if (!bootstrapServers.isEmpty())
            CompletableFuture.supplyAsync(
                    () -> {
                      var newAdmin = Admin.of(bootstrapServers);
                      var brokerIds = newAdmin.brokerIds();
                      var previous = context.replace(newAdmin);
                      previous.ifPresent(
                          admin -> org.astraea.common.Utils.swallowException(admin::close));
                      return "succeed to connect to "
                          + bootstrapServers
                          + ", and there are "
                          + brokerIds.size()
                          + " nodes";
                    })
                .whenComplete(console::append);
        });
    return tab;
  }
}
