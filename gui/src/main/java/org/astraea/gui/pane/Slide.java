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
package org.astraea.gui.pane;

import java.util.Map;
import java.util.stream.Collectors;
import javafx.geometry.Side;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;

public interface Slide {
  static Slide of(Side side, Map<String, Node> nodes) {
    var pane = new TabPane();
    pane.getTabs()
        .setAll(
            nodes.entrySet().stream()
                .map(
                    n -> {
                      var tab = new Tab(n.getKey());
                      tab.setContent(n.getValue());
                      return tab;
                    })
                .collect(Collectors.toList()));
    pane.setSide(side);
    pane.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
    return () -> pane;
  }

  Parent node();
}
