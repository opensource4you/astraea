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
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;

public interface TextInput {

  static TextInput required(String key, EditableText text) {
    var label = new Label(key + "*");
    label.setFont(Font.font("Verdana", FontWeight.EXTRA_BOLD, 12));
    return of(label, () -> key, text.node(), text::text, true);
  }

  static TextInput of(String selected, Set<String> keys, EditableText text) {
    var box = new ComboBox<>(FXCollections.observableArrayList(keys.toArray(String[]::new)));
    box.getSelectionModel().selectFirst();
    box.setValue(selected);
    // TODO: fix this hardcode
    box.setButtonCell(
        new ListCell<>() {
          @Override
          public void updateItem(String item, boolean empty) {
            super.updateItem(item, empty);
            if (item != null) {
              setText(item);
              setAlignment(Pos.BASELINE_RIGHT);
              setPadding(new Insets(5, 0, 5, 0));
            }
          }
        });
    var current = new AtomicReference<>(box.getValue());
    box.valueProperty().addListener((o, previous, now) -> current.set(now));
    return of(box, current::get, text.node(), text::text, false);
  }

  static TextInput of(String key, EditableText text) {
    var label = new Label(key);
    return of(label, () -> key, text.node(), text::text, false);
  }

  private static TextInput of(
      Node keyNode,
      Supplier<String> key,
      Node valueNode,
      Supplier<Optional<String>> value,
      boolean required) {
    return new TextInput() {
      @Override
      public void disable() {
        if (Platform.isFxApplicationThread()) {
          keyNode.setDisable(true);
          valueNode.setDisable(true);
        } else {
          Platform.runLater(
              () -> {
                keyNode.setDisable(true);
                valueNode.setDisable(true);
              });
        }
      }

      @Override
      public void enable() {
        if (Platform.isFxApplicationThread()) {
          keyNode.setDisable(false);
          valueNode.setDisable(false);
        } else {
          Platform.runLater(
              () -> {
                keyNode.setDisable(false);
                valueNode.setDisable(false);
              });
        }
      }

      @Override
      public boolean required() {
        return required;
      }

      @Override
      public String key() {
        return key.get();
      }

      @Override
      public Optional<String> value() {
        return value.get();
      }

      @Override
      public Node keyNode() {
        return keyNode;
      }

      @Override
      public Node valueNode() {
        return valueNode;
      }
    };
  }

  void disable();

  void enable();

  boolean required();

  String key();

  Optional<String> value();

  Node keyNode();

  Node valueNode();
}
