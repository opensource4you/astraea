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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.CheckBox;
import javafx.scene.input.KeyCode;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.Pane;
import org.astraea.common.function.Bi3Function;
import org.astraea.gui.Logger;
import org.astraea.gui.Query;
import org.astraea.gui.box.VBox;
import org.astraea.gui.button.Button;
import org.astraea.gui.button.SelectBox;
import org.astraea.gui.table.TableViewer;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.NoneditableText;

/** a template layout for all tabs. */
public class PaneBuilder {

  public static PaneBuilder of() {
    return new PaneBuilder();
  }

  // ---------------------------------[first control]---------------------------------//

  private SelectBox selectBox;

  private final Map<NoneditableText, EditableText> inputKeyAndFields = new LinkedHashMap<>();

  private Button actionButton = Button.of("REFRESH");

  private final EditableText console = EditableText.multiline().build();

  private TableViewer tableViewer = null;
  private Node motherOfTableView = null;

  private BiFunction<Input, Logger, CompletionStage<List<Map<String, Object>>>> buttonAction = null;
  private BiFunction<Input, Logger, CompletionStage<Void>> buttonListener = null;

  // ---------------------------------[second control]---------------------------------//

  private final Map<NoneditableText, EditableText> secondInputKeyAndFields = new LinkedHashMap<>();

  private Button tableViewActionButton = Button.disabled("EXECUTE");

  private Bi3Function<List<Map<String, Object>>, Input, Logger, CompletionStage<Void>>
      tableViewAction = null;

  private PaneBuilder() {}

  public PaneBuilder selectBox(SelectBox selectBox) {
    this.selectBox = selectBox;
    return this;
  }

  public PaneBuilder input(NoneditableText key, EditableText value) {
    inputKeyAndFields.put(key, value);
    return this;
  }

  public PaneBuilder input(Map<NoneditableText, EditableText> inputs) {
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
        EditableText.singleLine()
            .hint(
                "press ENTER to query. example: topic=chia && size>10GB || *timestamp*>=2022-10-22T04:57:43.530")
            .build();

    tableViewer =
        TableViewer.builder()
            .querySupplier(() -> queryField.text().map(Query::of).orElse(Query.ALL))
            .filteredDataListener(
                List.of((ignored, data) -> console.append("total: " + data.size())))
            .build();

    queryField
        .node()
        .setOnKeyPressed(
            key -> {
              if (key.getCode() == KeyCode.ENTER) tableViewer.refresh();
            });

    var borderPane = new BorderPane();
    borderPane.setTop(queryField.node());
    borderPane.setCenter(tableViewer.node());
    motherOfTableView = borderPane;
    return this;
  }

  public PaneBuilder buttonListener(
      BiFunction<Input, Logger, CompletionStage<Void>> buttonListener) {
    this.buttonListener = buttonListener;
    return this;
  }

  public PaneBuilder tableViewAction(
      Map<NoneditableText, EditableText> inputs,
      String buttonName,
      Bi3Function<List<Map<String, Object>>, Input, Logger, CompletionStage<Void>> action) {
    // always disable the input fields
    inputs.keySet().forEach(NoneditableText::disable);
    inputs.values().forEach(EditableText::disable);
    secondInputKeyAndFields.putAll(inputs);
    tableViewActionButton = Button.disabled(buttonName);
    tableViewAction = action;
    return this;
  }

  public Pane build() {
    // step.1 layout
    var nodes = new ArrayList<Node>();
    if (selectBox != null) nodes.add(selectBox.node());
    if (!inputKeyAndFields.isEmpty()) {
      var ns =
          inputKeyAndFields.entrySet().stream()
              .flatMap(entry -> Stream.of(entry.getKey().node(), entry.getValue().node()))
              .collect(Collectors.toList());
      var lattice = Lattice.of(ns, inputKeyAndFields.size() <= 3 ? 2 : 6);
      nodes.add(lattice.node());
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
                  secondInputKeyAndFields.keySet().forEach(NoneditableText::enable);
                  secondInputKeyAndFields.values().forEach(EditableText::enable);
                } else {
                  tableViewActionButton.disable();
                  secondInputKeyAndFields.keySet().forEach(NoneditableText::disable);
                  secondInputKeyAndFields.values().forEach(EditableText::disable);
                }
              });
      tableViewActionButton.setOnAction(
          event -> {
            var items = tableViewer.filteredData();
            var text =
                secondInputKeyAndFields.entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey().text(), e -> e.getValue().text()));
            var input =
                new Input() {
                  @Override
                  public List<String> selectedKeys() {
                    return List.of();
                  }

                  @Override
                  public Map<String, Optional<String>> texts() {
                    return text;
                  }
                };
            try {
              checkbox.setSelected(false);

              var requiredNonexistentKeys =
                  secondInputKeyAndFields.entrySet().stream()
                      .filter(e -> !e.getValue().valid())
                      .map(e -> e.getKey().text())
                      .collect(Collectors.toSet());
              if (!requiredNonexistentKeys.isEmpty()) {
                console.text("Please define required fields: " + requiredNonexistentKeys);
                return;
              }
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
              Lattice.of(
                      secondInputKeyAndFields.entrySet().stream()
                          .flatMap(
                              entry -> Stream.of(entry.getKey().node(), entry.getValue().node()))
                          .collect(Collectors.toList()),
                      6)
                  .node(),
              tableViewActionButton));
    }

    nodes.add(console.node());

    // step.2 event
    Runnable handler =
        () -> {
          var requiredNonexistentKeys =
              inputKeyAndFields.entrySet().stream()
                  .filter(e -> !e.getValue().valid())
                  .map(e -> e.getKey().text())
                  .collect(Collectors.toSet());
          if (!requiredNonexistentKeys.isEmpty()) {
            console.text("Please define required fields: " + requiredNonexistentKeys);
            return;
          }

          var rawTexts =
              inputKeyAndFields.entrySet().stream()
                  .collect(Collectors.toMap(e -> e.getKey().text(), e -> e.getValue().text()));
          var input =
              new Input() {
                @Override
                public List<String> selectedKeys() {
                  return selectBox == null ? List.of() : selectBox.selectedKeys();
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
