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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.geometry.HPos;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Pane;
import org.astraea.gui.text.TextInput;

public interface MultiInput {

  private static Pane gridTextInput(List<TextInput> items) {
    var grid = new GridPane();
    for (var i = 0; i != items.size(); ++i) {
      var keyNode = items.get(i).keyNode();
      var valueNode = items.get(i).valueNode();
      GridPane.setHalignment(keyNode, HPos.RIGHT);
      GridPane.setMargin(keyNode, new Insets(5, 5, 5, 5));
      grid.add(keyNode, 0, i);

      GridPane.setHalignment(valueNode, HPos.LEFT);
      GridPane.setMargin(valueNode, new Insets(5, 5, 5, 5));
      grid.add(valueNode, 1, i);
    }
    grid.setAlignment(Pos.CENTER);
    return grid;
  }

  static MultiInput of(List<TextInput> textInputs) {
    var box = gridTextInput(textInputs);
    return new MultiInput() {
      @Override
      public void disable() {
        textInputs.forEach(TextInput::disable);
      }

      @Override
      public void enable() {
        textInputs.forEach(TextInput::enable);
      }

      @Override
      public Node node() {
        return box;
      }

      @Override
      public Map<String, Optional<String>> contents() {
        return textInputs.stream().collect(Collectors.toMap(TextInput::key, TextInput::value));
      }

      @Override
      public Set<String> invalidKeys() {
        return Stream.concat(
                textInputs.stream()
                    .filter(input -> input.required() && input.value().isEmpty())
                    .map(TextInput::key),
                textInputs.stream()
                    .map(TextInput::key)
                    .collect(Collectors.groupingBy(i -> i))
                    .entrySet()
                    .stream()
                    .filter(e -> e.getValue().size() > 1)
                    .map(Map.Entry::getKey))
            .collect(Collectors.toSet());
      }
    };
  }

  void disable();

  void enable();

  Node node();

  Map<String, Optional<String>> contents();

  Set<String> invalidKeys();
}
