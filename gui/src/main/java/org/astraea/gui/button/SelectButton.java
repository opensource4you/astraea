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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javafx.application.Platform;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ScrollPane;
import org.astraea.gui.box.VBox;

public interface SelectButton {

  static SelectButton withScroll() {
    var pane = new ScrollPane();
    pane.setPrefWidth(150);
    return new SelectButton() {
      private final Set<String> selectedValues = Collections.synchronizedSet(new HashSet<>());

      private volatile List<CheckBox> boxes = List.of();

      @Override
      public Set<String> selectedContent() {
        return Set.copyOf(selectedValues);
      }

      @Override
      public void set(List<String> names) {
        selectedValues.clear();
        var newBoxes =
            names.stream()
                .map(
                    name -> {
                      var box = new CheckBox(name);
                      box.setIndeterminate(false);
                      boolean selected =
                          boxes.stream()
                              .filter(b -> b.getText().equals(name))
                              .map(CheckBox::isSelected)
                              .findFirst()
                              .orElse(true);
                      if (selected) selectedValues.add(name);
                      box.selectedProperty()
                          .addListener(
                              (o, previous, now) -> {
                                if (box.isSelected()) selectedValues.add(name);
                                else selectedValues.remove(name);
                              });
                      if (Platform.isFxApplicationThread()) box.setSelected(selected);
                      else Platform.runLater(() -> box.setSelected(selected));
                      return box;
                    })
                .collect(Collectors.toList());
        var box = VBox.of(Pos.TOP_LEFT, newBoxes.toArray(Node[]::new));
        if (Platform.isFxApplicationThread()) pane.setContent(box);
        else Platform.runLater(() -> pane.setContent(box));
        this.boxes = newBoxes;
      }

      public void makeAllSelected() {
        boxes.forEach(b -> b.setSelected(true));
      }

      public void makeAllUnselected() {
        boxes.forEach(b -> b.setSelected(false));
      }

      public Node node() {
        return pane;
      }
    };
  }

  Set<String> selectedContent();

  void set(List<String> names);

  void makeAllSelected();

  void makeAllUnselected();

  Node node();
}
