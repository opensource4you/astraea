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

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import javafx.application.Platform;
import javafx.scene.Node;
import org.astraea.gui.text.TextInput;

public class Tab extends javafx.scene.control.Tab {

  /**
   * create a tab having dynamical content. The content is changed when the tag is selected.
   *
   * @param name tab name
   * @param nodeSupplier offers the newest content
   * @return tab
   */
  public static Tab dynamic(String name, Supplier<CompletionStage<Node>> nodeSupplier) {
    var t = new Tab(name);
    t.setOnSelectionChanged(
        ignored -> {
          try {
            if (t.isSelected())
              nodeSupplier
                  .get()
                  .whenComplete(
                      (r, e) -> {
                        if (e != null)
                          t.content(
                              TextInput.singleLine()
                                  .defaultValue(e.getCause().getMessage())
                                  .build()
                                  .node());
                        else t.content(r);
                      });
          } catch (IllegalArgumentException e) {
            t.content(
                TextInput.singleLine().defaultValue(e.getCause().getMessage()).build().node());
          }
        });
    return t;
  }

  public static Tab of(String name, Node node) {
    var t = new Tab(name);
    t.setContent(node);
    return t;
  }

  private Tab(String name) {
    super(name);
  }

  public void content(Node node) {
    if (Platform.isFxApplicationThread()) setContent(node);
    else Platform.runLater(() -> setContent(node));
  }
}
