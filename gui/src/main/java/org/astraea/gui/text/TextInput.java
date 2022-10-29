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
package org.astraea.gui.text;

import java.util.Optional;
import java.util.function.Supplier;
import javafx.geometry.HPos;
import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;

public interface TextInput {

  static TextInput of(String key) {
    var label = new Label(key);
    var textField = new TextField();
    return of(
        label,
        label::getText,
        textField,
        () -> Optional.ofNullable(textField.getText()).filter(s -> !s.isBlank()));
  }

  private static TextInput of(
      Node keyNode, Supplier<String> key, Node valueNode, Supplier<Optional<String>> value) {
    var pane = new GridPane();
    GridPane.setHalignment(keyNode, HPos.RIGHT);
    GridPane.setMargin(keyNode, new Insets(10, 5, 10, 15));
    pane.add(keyNode, 0, 0);
    GridPane.setHalignment(valueNode, HPos.LEFT);
    GridPane.setMargin(valueNode, new Insets(10, 5, 10, 15));
    pane.add(valueNode, 1, 0);
    return new TextInput() {
      @Override
      public String key() {
        return key.get();
      }

      @Override
      public Optional<String> value() {
        return value.get();
      }

      @Override
      public Node node() {
        return pane;
      }
    };
  }

  String key();

  Optional<String> value();

  Node node();
}
