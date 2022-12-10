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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javafx.geometry.Side;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.layout.BorderPane;

public interface Slide<T extends Node> {
  static <T extends Node> Slide<T> of(Side side, Map<String, T> nodes) {
    return of(side, nodes, (i, j) -> {});
  }

  static <T extends Node> Slide<T> of(
      Side side, Map<String, T> nodes, BiConsumer<String, T> selectAction) {
    if (nodes.isEmpty()) throw new IllegalArgumentException("empty node!!!");
    if (nodes.size() == 1) {
      var pane = new BorderPane();
      var entry = nodes.entrySet().iterator().next();
      pane.setCenter(entry.getValue());
      selectAction.accept(entry.getKey(), entry.getValue());
      return new Slide<>() {
        @Override
        public Parent node() {
          return pane;
        }

        @Override
        public Map.Entry<String, T> selected() {
          return entry;
        }
      };
    }
    var pane = new TabPane();
    var tabs =
        nodes.entrySet().stream()
            .map(
                n -> {
                  var tab = new Tab(n.getKey());
                  tab.setContent(n.getValue());
                  return tab;
                })
            .collect(Collectors.toList());

    var currentEntry = new AtomicReference<Map.Entry<String, T>>();

    Consumer<String> triggerAction =
        s ->
            nodes.entrySet().stream()
                .filter(e -> e.getKey().equals(s))
                .findFirst()
                .ifPresent(
                    e -> {
                      currentEntry.set(e);
                      selectAction.accept(e.getKey(), e.getValue());
                    });

    // trigger the update now
    triggerAction.accept(tabs.get(0).getText());
    pane.getTabs().setAll(tabs);
    pane.setSide(side);
    pane.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
    // trigger the update when selected tab is changed
    pane.getSelectionModel()
        .selectedItemProperty()
        .addListener((ov, previous, now) -> triggerAction.accept(now.getText()));
    return new Slide<>() {

      @Override
      public Parent node() {
        return pane;
      }

      @Override
      public Map.Entry<String, T> selected() {
        return currentEntry.get();
      }
    };
  }

  Parent node();

  Map.Entry<String, T> selected();
}
