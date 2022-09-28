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
import javafx.geometry.Insets;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import javafx.scene.layout.VBox;
import org.astraea.common.VersionUtils;

public class AboutTab {

  public static Tab of(Context context) {
    var tab = new Tab("about");
    var ns =
        List.of(
            new Label("Version: " + VersionUtils.VERSION),
            new Label("Revision: " + VersionUtils.REVISION),
            new Label("Builder: " + VersionUtils.BUILDER),
            new Label("Date: " + VersionUtils.DATE),
            new Label("Web: https://github.com/skiptests/astraea"));
    var pane = new VBox(ns.size());
    pane.setPadding(new Insets(15));
    pane.getChildren().setAll(ns);
    tab.setContent(pane);
    return tab;
  }
}
