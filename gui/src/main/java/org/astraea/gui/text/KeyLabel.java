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

import javafx.application.Platform;
import javafx.scene.Node;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;

public interface KeyLabel extends Comparable<KeyLabel> {
  static KeyLabel of(String key) {
    var label = new javafx.scene.control.Label(key);
    return new KeyLabel() {

      @Override
      public Node node() {
        return label;
      }

      @Override
      public String key() {
        return key;
      }

      @Override
      public boolean highlight() {
        return false;
      }

      @Override
      public void text(String text) {
        if (Platform.isFxApplicationThread()) label.setText(text);
        else Platform.runLater(() -> label.setText(text));
      }
    };
  }

  static KeyLabel highlight(String key) {
    var label = new javafx.scene.control.Label(key + "*");
    label.setFont(Font.font("Verdana", FontWeight.EXTRA_BOLD, 12));
    return new KeyLabel() {

      @Override
      public Node node() {
        return label;
      }

      @Override
      public String key() {
        return key;
      }

      @Override
      public boolean highlight() {
        return true;
      }

      @Override
      public void text(String text) {
        if (Platform.isFxApplicationThread()) label.setText(text);
        else Platform.runLater(() -> label.setText(text));
      }
    };
  }

  Node node();

  String key();

  boolean highlight();

  void text(String text);

  @Override
  default int compareTo(KeyLabel o) {
    return key().compareTo(o.key());
  }
}
