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

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.scene.Node;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;

public interface NoneditableText {

  static NoneditableText of(Set<String> keys) {
    if (keys.isEmpty()) throw new IllegalArgumentException("empty keys is illegal");
    var box = new ComboBox<>(FXCollections.observableArrayList(keys.toArray(String[]::new)));
    box.getSelectionModel().selectFirst();
    var current = new AtomicReference<>(box.getValue());
    box.valueProperty().addListener((o, previous, now) -> current.set(now));
    return new NoneditableText() {

      @Override
      public Node node() {
        return box;
      }

      @Override
      public String text() {
        return current.get();
      }

      @Override
      public void disable() {
        if (Platform.isFxApplicationThread()) box.setDisable(true);
        else Platform.runLater(() -> box.setDisable(true));
      }

      @Override
      public void enable() {
        if (Platform.isFxApplicationThread()) box.setDisable(false);
        else Platform.runLater(() -> box.setDisable(false));
      }
    };
  }

  static NoneditableText of(String key) {
    return of(new Label(key), () -> key);
  }

  static NoneditableText highlight(String key) {
    var label = new javafx.scene.control.Label(key + "*");
    label.setFont(Font.font("Verdana", FontWeight.EXTRA_BOLD, 12));
    return of(label, () -> key);
  }

  private static NoneditableText of(Node node, Supplier<String> textSupplier) {
    return new NoneditableText() {

      @Override
      public Node node() {
        return node;
      }

      @Override
      public String text() {
        return textSupplier.get();
      }
    };
  }

  Node node();

  String text();

  default void disable() {}

  default void enable() {}
}
