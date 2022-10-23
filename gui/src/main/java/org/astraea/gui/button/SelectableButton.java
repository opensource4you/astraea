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

import java.util.ArrayList;
import java.util.Collection;
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
import javafx.scene.input.KeyCode;
import org.astraea.common.Utils;
import org.astraea.gui.box.HBox;
import org.astraea.gui.box.VBox;
import org.astraea.gui.text.TextField;

public interface SelectableButton {

  static SelectableButton withScroll() {
    var pane = new ScrollPane();
    pane.setPrefWidth(150);
    pane.setPrefWidth(300);
    return new SelectableButton() {
      private final Set<String> selectedValues = Collections.synchronizedSet(new HashSet<>());

      private volatile List<CheckBox> boxes = List.of();

      @Override
      public Set<String> selectedContent() {
        return Set.copyOf(selectedValues);
      }

      @Override
      public void set(Collection<String> names) {
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
        var wildcardText = TextField.builder().hint("*thread*").build();
        wildcardText.setOnKeyPressed(
            event -> {
              if (event.getCode() == KeyCode.ENTER) {
                var context = wildcardText.getText();
                Runnable action;
                if (context != null && !context.isBlank()) {
                  var pattern = Utils.wildcardToPattern(context);
                  action =
                      () ->
                          this.boxes.forEach(
                              b -> b.setSelected(pattern.matcher(b.getText()).matches()));

                } else action = () -> this.boxes.forEach(b -> b.setSelected(false));
                if (Platform.isFxApplicationThread()) action.run();
                else Platform.runLater(action);
              }
            });
        var nodes = new ArrayList<Node>();
        nodes.add(HBox.of(Pos.TOP_LEFT, wildcardText));
        nodes.addAll(newBoxes);
        var box = VBox.of(Pos.TOP_LEFT, nodes.toArray(Node[]::new));
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

  void set(Collection<String> names);

  void makeAllSelected();

  void makeAllUnselected();

  Node node();
}
