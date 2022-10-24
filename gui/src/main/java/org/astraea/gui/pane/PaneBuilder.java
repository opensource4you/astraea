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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.CheckBox;
import javafx.scene.input.KeyCode;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.Pane;
import org.astraea.common.function.Bi3Function;
import org.astraea.gui.Logger;
import org.astraea.gui.Query;
import org.astraea.gui.box.HBox;
import org.astraea.gui.box.VBox;
import org.astraea.gui.button.Button;
import org.astraea.gui.button.RadioButton;
import org.astraea.gui.table.TableViewer;
import org.astraea.gui.text.Label;
import org.astraea.gui.text.TextArea;
import org.astraea.gui.text.TextField;

/** a template layout for all tabs. */
public class PaneBuilder {

  public static PaneBuilder of() {
    return new PaneBuilder();
  }

  // ---------------------------------[first control]---------------------------------//

  private List<RadioButton> radioButtons = new ArrayList<>();

  private final Map<Label, TextField> inputKeyAndFields = new LinkedHashMap<>();

  private Button actionButton = Button.of("REFRESH");

  private final TextArea console = TextArea.of();

  private TableViewer tableViewer = null;
  private Node motherOfTableView = null;

  private BiFunction<Input, Logger, CompletionStage<List<Map<String, Object>>>> buttonAction = null;
  private BiFunction<Input, Logger, CompletionStage<Void>> buttonListener = null;

  // ---------------------------------[second control]---------------------------------//

  private final Map<Label, TextField> secondInputKeyAndFields = new LinkedHashMap<>();

  private Button tableViewActionButton = Button.disabled("EXECUTE");

  private Bi3Function<List<Map<String, Object>>, Map<String, String>, Logger, CompletionStage<Void>>
      tableViewAction = null;

  private PaneBuilder() {}

  public PaneBuilder singleRadioButtons(Object[] objs) {
    return singleRadioButtons(Arrays.asList(objs));
  }

  public PaneBuilder singleRadioButtons(List<Object> objs) {
    if (objs.isEmpty()) return this;
    radioButtons = RadioButton.single(objs);
    return this;
  }

  public PaneBuilder multiRadioButtons(List<Object> objs) {
    if (objs.isEmpty()) return this;
    radioButtons = RadioButton.multi(objs);
    return this;
  }

  public PaneBuilder input(Label key, TextField value) {
    inputKeyAndFields.put(key, value);
    return this;
  }

  public PaneBuilder input(Map<Label, TextField> inputs) {
    inputKeyAndFields.putAll(inputs);
    return this;
  }

  public PaneBuilder buttonName(String name) {
    actionButton = Button.of(name);
    return this;
  }

  public PaneBuilder buttonAction(
      BiFunction<Input, Logger, CompletionStage<List<Map<String, Object>>>> buttonAction) {
    this.buttonAction = buttonAction;
    var queryField =
        TextField.builder().hint("c0=aa||c1<20||c2>30MB||c3>=2022-10-22T04:57:43.530").build();
    var sizeLabel = Label.of("");

    tableViewer =
        TableViewer.builder()
            .querySupplier(() -> queryField.text().map(Query::of).orElse(Query.ALL))
            .filteredDataListener(
                List.of((ignored, data) -> sizeLabel.text("total: " + data.size())))
            .build();

    queryField.setOnKeyPressed(
        key -> {
          if (key.getCode() == KeyCode.ENTER) tableViewer.refresh();
        });

    var borderPane = new BorderPane();
    borderPane.setTop(queryField);
    borderPane.setCenter(tableViewer.node());
    borderPane.setBottom(sizeLabel);
    BorderPane.setAlignment(sizeLabel, Pos.CENTER);
    motherOfTableView = borderPane;
    return this;
  }

  public PaneBuilder buttonListener(
      BiFunction<Input, Logger, CompletionStage<Void>> buttonListener) {
    this.buttonListener = buttonListener;
    return this;
  }

  public PaneBuilder tableViewAction(
      Map<Label, TextField> inputs,
      String buttonName,
      Bi3Function<List<Map<String, Object>>, Map<String, String>, Logger, CompletionStage<Void>>
          action) {
    secondInputKeyAndFields.putAll(inputs);
    tableViewActionButton = Button.disabled(buttonName);
    tableViewAction = action;
    return this;
  }

  public Pane build() {
    // step.1 layout
    var nodes = new ArrayList<Node>();
    if (!radioButtons.isEmpty()) nodes.add(HBox.of(Pos.CENTER, radioButtons.toArray(Node[]::new)));
    if (!inputKeyAndFields.isEmpty()) {
      var gridPane =
          inputKeyAndFields.size() <= 3
              ? GridPane.singleColumn(inputKeyAndFields)
              : GridPane.of(inputKeyAndFields, 3);
      nodes.add(gridPane);
    }
    nodes.add(actionButton);
    if (motherOfTableView != null) nodes.add(motherOfTableView);
    // ---------------------------------[second control layout]---------------------------------//
    if (tableViewer != null && tableViewAction != null) {
      var checkbox = new CheckBox("enable");
      checkbox
          .selectedProperty()
          .addListener(
              (observable, oldValue, newValue) -> {
                if (checkbox.isSelected()) {
                  tableViewActionButton.enable();
                  secondInputKeyAndFields.values().forEach(TextField::enable);
                } else {
                  tableViewActionButton.disable();
                  secondInputKeyAndFields.values().forEach(TextField::disable);
                }
              });
      tableViewActionButton.setOnAction(
          event -> {
            var items = tableViewer.filteredData();
            var input =
                secondInputKeyAndFields.entrySet().stream()
                    .flatMap(e -> e.getValue().text().stream().map(v -> Map.entry(e.getKey(), v)))
                    .collect(Collectors.toMap(e -> e.getKey().key(), Map.Entry::getValue));
            try {
              checkbox.setSelected(false);
              tableViewAction
                  .apply(items, input, console::append)
                  .whenComplete((data, e) -> console.text(e));
            } catch (Exception e) {
              console.text(e);
            }
          });

      nodes.add(
          VBox.of(
              Pos.CENTER,
              checkbox,
              GridPane.singleColumn(secondInputKeyAndFields),
              tableViewActionButton));
    }

    nodes.add(console);

    // step.2 event
    Runnable handler =
        () -> {
          var multiSelectedRadio =
              radioButtons.stream()
                  .filter(RadioButton::isSelected)
                  .flatMap(r -> r.selectedObject().stream())
                  .collect(Collectors.toList());
          var requiredNonexistentKeys =
              inputKeyAndFields.entrySet().stream()
                  .filter(entry -> entry.getKey().highlight())
                  .filter(entry -> entry.getValue().text().isEmpty())
                  .map(e -> e.getKey().key())
                  .collect(Collectors.toSet());
          if (!requiredNonexistentKeys.isEmpty()) {
            console.text("Please define required fields: " + requiredNonexistentKeys);
            return;
          }

          var rawTexts =
              inputKeyAndFields.entrySet().stream()
                  .collect(Collectors.toMap(e -> e.getKey().key(), e -> e.getValue().text()));
          var input =
              new Input() {
                @Override
                @SuppressWarnings("unchecked")
                public <T> List<T> multiSelectedRadios(List<T> defaultObjs) {
                  return multiSelectedRadio.isEmpty() ? defaultObjs : (List<T>) multiSelectedRadio;
                }

                @Override
                public Map<String, Optional<String>> texts() {
                  return rawTexts;
                }
              };

          // nothing to do
          if (buttonAction == null && buttonListener == null) return;

          console.cleanup();
          actionButton.disable();
          try {
            if (buttonAction != null)
              buttonAction
                  .apply(input, console::append)
                  .whenComplete(
                      (data, e) -> {
                        try {
                          if (data != null && tableViewer != null) tableViewer.data(data);
                          console.text(e);
                        } finally {
                          actionButton.enable();
                        }
                      });
            if (buttonListener != null)
              buttonListener
                  .apply(input, console::append)
                  .whenComplete(
                      (data, e) -> {
                        try {
                          console.text(e);
                        } finally {
                          actionButton.enable();
                        }
                      });

          } catch (Exception e) {
            console.text(e);
            actionButton.enable();
          }
        };

    actionButton.setOnAction(ignored -> handler.run());
    return VBox.of(Pos.CENTER, nodes.toArray(Node[]::new));
  }
}
