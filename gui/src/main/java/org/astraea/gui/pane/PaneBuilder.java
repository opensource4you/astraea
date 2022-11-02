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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import javafx.scene.Node;
import javafx.scene.control.CheckBox;
import javafx.scene.input.KeyCode;
import org.astraea.common.function.Bi3Function;
import org.astraea.gui.Logger;
import org.astraea.gui.button.Click;
import org.astraea.gui.button.SelectBox;
import org.astraea.gui.table.TableViewer;
import org.astraea.gui.text.EditableText;

/** a template layout for all tabs. */
public class PaneBuilder {

  public static PaneBuilder of() {
    return new PaneBuilder();
  }

  // ---------------------------------[first control]---------------------------------//

  private SelectBox selectBox;

  private Lattice lattice = null;

  private Click click;

  private final EditableText console = EditableText.multiline().build();

  private TableViewer tableViewer = null;

  private BiFunction<Input, Logger, CompletionStage<List<Map<String, Object>>>> tableRefresher =
      null;
  private BiFunction<Input, Logger, CompletionStage<Void>> clickListener = null;

  // ---------------------------------[second control]---------------------------------//

  private Lattice secondLattice = null;

  private Click tableViewClick;

  private Bi3Function<List<Map<String, Object>>, Input, Logger, CompletionStage<Void>>
      tableViewAction = null;

  private PaneBuilder() {}

  public PaneBuilder selectBox(SelectBox selectBox) {
    this.selectBox = selectBox;
    return this;
  }

  public PaneBuilder lattice(Lattice lattice) {
    this.lattice = lattice;
    return this;
  }

  public PaneBuilder clickFunction(
      String clickName,
      BiFunction<Input, Logger, CompletionStage<List<Map<String, Object>>>> tableRefresher) {
    this.click = Click.of(clickName);
    this.tableRefresher = tableRefresher;
    this.tableViewer = TableViewer.of();
    this.tableViewer.filteredDataListener(
        (ignored, data) -> console.append("total: " + data.size()));
    return this;
  }

  public PaneBuilder clickListener(
      String clickName, BiFunction<Input, Logger, CompletionStage<Void>> buttonListener) {
    this.click = Click.of(clickName);
    this.clickListener = buttonListener;
    return this;
  }

  public PaneBuilder tableViewAction(
      Lattice lattice,
      String buttonName,
      Bi3Function<List<Map<String, Object>>, Input, Logger, CompletionStage<Void>> action) {
    this.secondLattice = lattice;
    tableViewClick = Click.disabled(buttonName);
    tableViewAction = action;
    return this;
  }

  public Node build() {
    // step.1 layout
    var nodes = new ArrayList<Node>();
    if (selectBox != null) nodes.add(selectBox.node());
    if (lattice != null) nodes.add(lattice.node());
    nodes.add(click.node());
    if (tableViewer != null) nodes.add(tableViewer.node());

    // step.2 click after table view
    if (tableViewer != null && tableViewAction != null) {
      var checkbox = new CheckBox("enable");
      checkbox
          .selectedProperty()
          .addListener(
              (observable, oldValue, newValue) -> {
                if (checkbox.isSelected()) {
                  tableViewClick.enable();
                  if (secondLattice != null) secondLattice.enable();
                } else {
                  tableViewClick.disable();
                  if (secondLattice != null) secondLattice.disable();
                }
              });
      tableViewClick.action(
          () -> {
            var items = tableViewer.filteredData();
            var text =
                secondLattice == null
                    ? Map.<String, Optional<String>>of()
                    : secondLattice.contents();
            // the click for table view requires only user text inputs.
            var input = Input.of(List.of(), text);

            checkbox.setDisable(true);
            checkbox.setSelected(false);
            try {
              var invalidKeys = secondLattice == null ? Set.of() : secondLattice.invalidKeys();
              if (!invalidKeys.isEmpty()) {
                console.text("please check fields: " + invalidKeys);
                return;
              }
              tableViewAction
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
      if (secondLattice == null) nodes.add(Lattice.vbox(List.of(checkbox, tableViewClick.node())));
      else nodes.add(Lattice.vbox(List.of(checkbox, secondLattice.node(), tableViewClick.node())));
    }

    nodes.add(console.node());

    // step.3 click before table view
    Runnable handler =
        () -> {
          // nothing to do
          if (tableRefresher == null && clickListener == null) return;

          var invalidKeys = lattice == null ? Set.of() : lattice.invalidKeys();
          if (!invalidKeys.isEmpty()) {
            console.text("please check fields: " + invalidKeys);
            return;
          }

          var input =
              Input.of(
                  selectBox == null ? List.of() : selectBox.selectedKeys(),
                  lattice == null ? Map.of() : lattice.contents());

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
            if (keyEvent.getCode() == KeyCode.ENTER) handler.run();
          });
    return Lattice.vbox(nodes);
  }
}
