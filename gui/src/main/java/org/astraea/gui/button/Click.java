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
package org.astraea.gui.button;

import javafx.application.Platform;
import javafx.scene.Node;

public interface Click {

  static Click of(String name) {
    return of(new javafx.scene.control.Button(name));
  }

  static Click disabled(String name) {
    var click = of(name);
    click.disable();
    return click;
  }

  private static Click of(javafx.scene.control.Button btn) {
    return new Click() {
      @Override
      public String name() {
        return btn.getText();
      }

      @Override
      public void action(Runnable runnable) {
        btn.setOnAction(ignored -> runnable.run());
      }

      @Override
      public Node node() {
        return btn;
      }

      @Override
      public void disable() {
        if (Platform.isFxApplicationThread()) btn.setDisable(true);
        else Platform.runLater(() -> btn.setDisable(true));
      }

      @Override
      public void enable() {
        if (Platform.isFxApplicationThread()) btn.setDisable(false);
        else Platform.runLater(() -> btn.setDisable(false));
      }
    };
  }

  String name();

  void action(Runnable runnable);

  Node node();

  void disable();

  void enable();
}
