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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.CheckBox;
import javafx.scene.input.KeyCode;
import javafx.scene.layout.Pane;
import javafx.scene.layout.VBox;
import org.astraea.common.function.Bi3Function;
import org.astraea.gui.Logger;
import org.astraea.gui.button.Click;
import org.astraea.gui.button.SelectBox;
import org.astraea.gui.table.TableViewer;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.TextInput;

/**
 * Layout: <br>
 * -----------[first part]----------- <br>
 * 1) (optional) select box <br>
 * 2) (optional) multi input <br>
 * 3) (required) click <br>
 * ------------------------------------ <br>
 * 4) (optional) table view <br>
 * -----------[second part]----------- <br>
 * 5) (optional) enable second control <br>
 * 6) (optional) second multi input <br>
 * 7) (optional) second click <br>
 * ------------------------------------ <br>
 * 8) (required) console <br>
 */
public class PaneBuilder {

  public static PaneBuilder of() {
    return new PaneBuilder(TableViewer.of());
  }

  public static PaneBuilder of(TableViewer tableViewer) {
    return new PaneBuilder(tableViewer);
  }

  private final EditableText console = EditableText.multiline().build();

  private final Pane root = vbox(List.of(console.node()));

  private final TableViewer tableViewer;

  private Node secondControl;

  private PaneBuilder(TableViewer tableViewer) {
    this.tableViewer = tableViewer;
  }

  public PaneBuilder firstPart(
      SelectBox selectBox,
      String clickName,
      BiFunction<Argument, Logger, CompletionStage<List<Map<String, Object>>>> tableRefresher) {
    return firstPart(selectBox, List.of(), clickName, TableRefresher.of(tableRefresher));
  }

  public PaneBuilder firstPart(
      List<TextInput> textInputs,
      String clickName,
      BiFunction<Argument, Logger, CompletionStage<List<Map<String, Object>>>> tableRefresher) {
    return firstPart(null, textInputs, clickName, TableRefresher.of(tableRefresher));
  }

  public PaneBuilder firstPart(
      String clickName,
      BiFunction<Argument, Logger, CompletionStage<List<Map<String, Object>>>> tableRefresher) {
    return firstPart(null, List.of(), clickName, TableRefresher.of(tableRefresher));
  }

  public PaneBuilder firstPart(
      SelectBox selectBox,
      List<TextInput> textInputs,
      String clickName,
      TableRefresher tableRefresher) {
    Objects.requireNonNull(clickName);
    Objects.requireNonNull(tableRefresher);
    var click = Click.of(clickName);
    var multiInput = textInputs.isEmpty() ? null : MultiInput.of(textInputs);
    root.getChildren()
        .add(
            0,
            vbox(
                Stream.of(
                        Optional.ofNullable(selectBox).map(SelectBox::node).stream(),
                        Optional.ofNullable(multiInput).map(MultiInput::node).stream(),
                        Optional.of(click.node()).stream())
                    .flatMap(s -> s)
                    .collect(Collectors.toList())));

    var indexOfTableViewer = 1;

    Runnable handler =
        () -> {
          var invalidKeys = multiInput == null ? Set.of() : multiInput.invalidKeys();
          if (!invalidKeys.isEmpty()) {
            console.text("please check fields: " + invalidKeys);
            return;
          }

          var argument =
              Argument.of(
                  selectBox == null ? List.of() : selectBox.selectedKeys(),
                  multiInput == null ? Map.of() : multiInput.contents());

          console.cleanup();
          click.disable();
          try {
            tableRefresher
                .apply(argument, console::append)
                .whenComplete(
                    (data, e) -> {
                      try {
                        if (data != null && !data.isEmpty()) {
                          if (!root.getChildren().contains(tableViewer.node()))
                            Platform.runLater(
                                () ->
                                    root.getChildren().add(indexOfTableViewer, tableViewer.node()));

                          if (secondControl != null && !root.getChildren().contains(secondControl))
                            Platform.runLater(
                                () ->
                                    root.getChildren().add(indexOfTableViewer + 1, secondControl));
                          tableViewer.data(data);
                        } else
                          Platform.runLater(
                              () -> {
                                root.getChildren().remove(tableViewer.node());
                                if (secondControl != null) root.getChildren().remove(secondControl);
                              });
                        console.text(e);
                      } catch (Exception e2) {
                        console.text(e2);
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
    tableViewer.keyAction(
        keyEvent -> {
          if (keyEvent.getCode() == KeyCode.ENTER) handler.run();
        });
    return this;
  }

  public PaneBuilder secondPart(
      String buttonName,
      Bi3Function<List<Map<String, Object>>, Argument, Logger, CompletionStage<Void>> action) {
    return secondPart(List.of(), buttonName, action);
  }

  public PaneBuilder secondPart(
      List<TextInput> textInputs,
      String buttonName,
      Bi3Function<List<Map<String, Object>>, Argument, Logger, CompletionStage<Void>> action) {
    Objects.requireNonNull(buttonName);
    Objects.requireNonNull(action);
    var multiInput = textInputs.isEmpty() ? null : MultiInput.of(textInputs);
    var tableViewClick = Click.disabled(buttonName);
    var checkbox = new CheckBox("enable");
    checkbox
        .selectedProperty()
        .addListener(
            (observable, oldValue, newValue) -> {
              if (checkbox.isSelected()) {
                tableViewClick.enable();
                if (multiInput != null) multiInput.enable();
              } else {
                tableViewClick.disable();
                if (multiInput != null) multiInput.disable();
              }
            });
    tableViewClick.action(
        () -> {
          var items = tableViewer.filteredData();
          if (items.isEmpty()) return;
          var text =
              multiInput == null ? Map.<String, Optional<String>>of() : multiInput.contents();
          // the click for table view requires only user text inputs.
          var input = Argument.of(List.of(), text);

          checkbox.setDisable(true);
          checkbox.setSelected(false);
          try {
            var invalidKeys = multiInput == null ? Set.of() : multiInput.invalidKeys();
            if (!invalidKeys.isEmpty()) {
              console.text("please check fields: " + invalidKeys);
              return;
            }
            action
                .apply(items, input, console::append)
                .whenComplete(
                    (data, e) -> {
                      checkbox.setDisable(false);
                      console.text(e);
                    });
          } catch (Exception e) {
            checkbox.setDisable(false);
            console.text(e);
          }
        });

    if (multiInput == null) secondControl = vbox(List.of(checkbox, tableViewClick.node()));
    else secondControl = vbox(List.of(checkbox, multiInput.node(), tableViewClick.node()));
    return this;
  }

  public Node build() {
    return root;
  }

  private static Pane vbox(List<Node> nodes) {
    var pane = new VBox(10);
    pane.setPadding(new Insets(15));
    pane.getChildren().setAll(nodes);
    pane.setAlignment(Pos.CENTER);
    return pane;
  }
}
