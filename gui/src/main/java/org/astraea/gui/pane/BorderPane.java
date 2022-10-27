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

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.ComboBox;
import org.astraea.common.Utils;
import org.astraea.gui.text.EditableText;

public class BorderPane extends javafx.scene.layout.BorderPane {

  public static BorderPane dynamic(
      Set<String> keys, Function<String, CompletionStage<Node>> nodeSupplier) {
    var box = new ComboBox<>(FXCollections.observableArrayList(keys.toArray(String[]::new)));
    var pane = new BorderPane();
    BorderPane.setAlignment(box, Pos.CENTER);
    pane.setTop(box);
    box.valueProperty()
        .addListener(
            (observable, oldValue, key) ->
                nodeSupplier
                    .apply(key)
                    .whenComplete(
                        (r, e) -> {
                          if (e != null)
                            pane.center(
                                EditableText.multiline()
                                    .defaultValue(Utils.toString(e))
                                    .build()
                                    .node());
                          else pane.center(r);
                        }));
    return pane;
  }

  private BorderPane() {}

  public void center(Node node) {
    if (Platform.isFxApplicationThread()) setCenter(node);
    else Platform.runLater(() -> setCenter(node));
  }
}
