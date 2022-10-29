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
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.CheckBox;
import javafx.scene.input.KeyCode;
import org.astraea.common.function.Bi3Function;
import org.astraea.gui.Logger;
import org.astraea.gui.button.Click;
import org.astraea.gui.button.SelectBox;
import org.astraea.gui.table.TableViewer;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.NoneditableText;

/** a template layout for all tabs. */
public class PaneBuilder {

  private static final String REFRESH_KEY = "REFRESH";

  public static PaneBuilder of() {
    return new PaneBuilder();
  }

  // ---------------------------------[first control]---------------------------------//

  private SelectBox selectBox;

  private final Map<NoneditableText, EditableText> inputKeyAndFields = new LinkedHashMap<>();

  private Click click = Click.of(REFRESH_KEY);

  private final EditableText console = EditableText.multiline().build();

  private TableViewer tableViewer = null;

  private BiFunction<Input, Logger, CompletionStage<List<Map<String, Object>>>> tableRefresher =
      null;
  private BiFunction<Input, Logger, CompletionStage<Void>> clickListener = null;

  // ---------------------------------[second control]---------------------------------//

  private final Map<NoneditableText, EditableText> secondInputKeyAndFields = new LinkedHashMap<>();

  private Click tableViewClick = Click.disabled("EXECUTE");

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

  public PaneBuilder clickName(String name) {
    click = Click.of(name);
    return this;
  }

  public PaneBuilder tableRefresher(
      BiFunction<Input, Logger, CompletionStage<List<Map<String, Object>>>> tableRefresher) {
    this.tableRefresher = tableRefresher;
    this.tableViewer = TableViewer.of();
    this.tableViewer.filteredDataListener(
        (ignored, data) -> console.append("total: " + data.size()));
    return this;
  }

  public PaneBuilder clickListener(
      BiFunction<Input, Logger, CompletionStage<Void>> buttonListener) {
    this.clickListener = buttonListener;
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
    tableViewClick = Click.disabled(buttonName);
    tableViewAction = action;
    return this;
  }

  public Node build() {
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
    nodes.add(click.node());
    if (tableViewer != null) nodes.add(tableViewer.node());
    // ---------------------------------[second control layout]---------------------------------//
    if (tableViewer != null && tableViewAction != null) {
      var checkbox = new CheckBox("enable");
      checkbox
          .selectedProperty()
          .addListener(
              (observable, oldValue, newValue) -> {
                if (checkbox.isSelected()) {
                  tableViewClick.enable();
                  secondInputKeyAndFields.keySet().forEach(NoneditableText::enable);
                  secondInputKeyAndFields.values().forEach(EditableText::enable);
                } else {
                  tableViewClick.disable();
                  secondInputKeyAndFields.keySet().forEach(NoneditableText::disable);
                  secondInputKeyAndFields.values().forEach(EditableText::disable);
                }
              });
      tableViewClick.action(
          () -> {
            var items = tableViewer.filteredData();
            var text =
                secondInputKeyAndFields.entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey().text(), e -> e.getValue().text()));
            var input = Input.of(List.of(), text);
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
          Lattice.singleColumn(
                  Pos.CENTER,
                  checkbox,
                  Lattice.of(
                          secondInputKeyAndFields.entrySet().stream()
                              .flatMap(
                                  entry ->
                                      Stream.of(entry.getKey().node(), entry.getValue().node()))
                              .collect(Collectors.toList()),
                          6)
                      .node(),
                  tableViewClick.node())
              .node());
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
          var input = Input.of(selectBox == null ? List.of() : selectBox.selectedKeys(), rawTexts);
          // nothing to do
          if (tableRefresher == null && clickListener == null) return;

          console.cleanup();
          click.disable();
          try {
            if (tableRefresher != null)
              tableRefresher
                  .apply(input, console::append)
                  .whenComplete(
                      (data, e) -> {
                        try {
                          if (data != null && tableViewer != null) tableViewer.data(data);
                          console.text(e);
                        } finally {
                          click.enable();
                        }
                      });
            if (clickListener != null)
              clickListener
                  .apply(input, console::append)
                  .whenComplete(
                      (data, e) -> {
                        try {
                          console.text(e);
                        } finally {
                          click.enable();
                        }
                      });

          } catch (Exception e) {
            console.text(e);
            click.enable();
          }
        };

    click.action(handler);
    if (tableViewer != null)
      tableViewer.keyAction(
          keyEvent -> {
            if (keyEvent.getCode() == KeyCode.ENTER) {
              // TODO: it is unstable to change action according to "string"
              // If the click action is used to fetch server data, we refresh all data
              // otherwise, we only filter the current data
              if (click.name().equals(REFRESH_KEY)) handler.run();
              else tableViewer.refresh();
            }
          });
    return Lattice.singleColumn(Pos.CENTER, nodes.toArray(Node[]::new)).node();
  }
}
