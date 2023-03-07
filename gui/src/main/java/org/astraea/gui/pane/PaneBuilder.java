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
import java.util.Optional;
import java.util.Set;
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
import org.astraea.gui.button.Click;
import org.astraea.gui.button.SelectBox;
import org.astraea.gui.table.TableViewer;
import org.astraea.gui.text.EditableText;

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

  public PaneBuilder firstPart(FirstPart part) {
    var click = Click.of(part.clickName());
    var multiInput =
        part.textInputs().isEmpty()
            ? Optional.<MultiInput>empty()
            : Optional.of(MultiInput.of(part.textInputs()));
    root.getChildren()
        .add(
            0,
            vbox(
                Stream.of(
                        part.selectBox().map(SelectBox::node).stream(),
                        multiInput.map(MultiInput::node).stream(),
                        Optional.of(click.node()).stream())
                    .flatMap(s -> s)
                    .collect(Collectors.toList())));

    var indexOfTableViewer = 1;

    Runnable handler =
        () -> {
          var invalidKeys = multiInput.map(MultiInput::invalidKeys).orElse(Set.of());
          if (!invalidKeys.isEmpty()) {
            console.text("please check fields: " + invalidKeys);
            return;
          }

          var argument =
              Argument.of(
                  part.selectBox().map(SelectBox::selectedKeys).orElse(List.of()),
                  multiInput.map(MultiInput::contents).orElse(Map.of()));

          console.cleanup();
          click.disable();
          try {
            part.tipRefresher()
                .apply(argument, console::append)
                .whenComplete((tips, e) -> tableViewer.tip(tips));
            part.tableRefresher()
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

  public PaneBuilder secondPart(SecondPart secondPart) {
    var multiInput =
        secondPart.textInputs().isEmpty()
            ? Optional.<MultiInput>empty()
            : Optional.of(MultiInput.of(secondPart.textInputs()));
    var tableViewClick = Click.disabled(secondPart.buttonName());
    var checkbox = new CheckBox("enable");
    checkbox
        .selectedProperty()
        .addListener(
            (observable, oldValue, newValue) -> {
              if (checkbox.isSelected()) {
                tableViewClick.enable();
                multiInput.ifPresent(MultiInput::enable);
              } else {
                tableViewClick.disable();
                multiInput.ifPresent(MultiInput::disable);
              }
            });
    tableViewClick.action(
        () -> {
          var items = tableViewer.filteredData();
          if (items.isEmpty()) return;
          var text = multiInput.map(MultiInput::contents).orElse(Map.of());
          // the click for table view requires only user text inputs.
          var input = Argument.of(List.of(), text);

          checkbox.setDisable(true);
          checkbox.setSelected(false);
          try {
            var invalidKeys = multiInput.map(MultiInput::invalidKeys).orElse(Set.of());
            if (!invalidKeys.isEmpty()) {
              console.text("please check fields: " + invalidKeys);
              return;
            }
            secondPart
                .action()
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

    secondControl =
        multiInput
            .map(m -> vbox(List.of(checkbox, m.node(), tableViewClick.node())))
            .orElseGet(() -> vbox(List.of(checkbox, tableViewClick.node())));
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
