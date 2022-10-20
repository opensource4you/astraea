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
package org.astraea.gui.table;

import java.util.Collection;
import java.util.Comparator;
import java.util.EventObject;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableColumnBase;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;

public class TableView extends javafx.scene.control.TableView<Map<String, Object>> {

  public static TableView copyable() {
    var tableView = new TableView();
    tableView.getSelectionModel().setCellSelectionEnabled(true);

    Function<EventObject, Map.Entry<Object, Object>> fetcher =
        event -> {
          var pos = tableView.getFocusModel().getFocusedCell();
          if (pos.getRow() < 0) return null;
          var item = tableView.getItems().get(pos.getRow());
          var value = pos.getTableColumn().getCellObservableValue(item).getValue();
          return Map.entry(item, value);
        };

    var keyForWindows = new KeyCodeCombination(KeyCode.C, KeyCombination.CONTROL_DOWN);

    var keyForMacos = new KeyCodeCombination(KeyCode.C, KeyCombination.SHORTCUT_DOWN);

    var copyAllForMacos = new KeyCodeCombination(KeyCode.A, KeyCombination.SHORTCUT_DOWN);
    var copyAllForWindows = new KeyCodeCombination(KeyCode.A, KeyCombination.CONTROL_DOWN);

    tableView.setOnKeyPressed(
        event -> {
          var result = fetcher.apply(event);
          if (result != null && (keyForWindows.match(event) || keyForMacos.match(event))) {
            if (event.getSource() instanceof TableView) {
              var clipboardContent = new ClipboardContent();
              clipboardContent.putString(result.getValue().toString());
              Clipboard.getSystemClipboard().setContent(clipboardContent);
            }
          }

          if (copyAllForWindows.match(event) || copyAllForMacos.match(event)) {
            var items = List.copyOf(tableView.getItems());
            var keys =
                tableView.getColumns().stream()
                    .map(TableColumnBase::getText)
                    .collect(Collectors.toList());
            var stringBuilder = new StringBuilder();
            stringBuilder.append(String.join(",", keys)).append("\n");
            items.forEach(
                item ->
                    stringBuilder
                        .append(
                            keys.stream()
                                .map(key -> item.getOrDefault(key, "").toString())
                                .collect(Collectors.joining(",")))
                        .append("\n"));
            if (stringBuilder.length() > 0) {
              var clipboardContent = new ClipboardContent();
              clipboardContent.putString(stringBuilder.toString());
              Clipboard.getSystemClipboard().setContent(clipboardContent);
            }
          }
        });
    return tableView;
  }

  private TableView() {
    super();
  }

  public void update(List<Map<String, Object>> data) {
    var columns =
        data.stream()
            .map(Map::keySet)
            .sorted(Comparator.comparingInt((Set<String> o) -> o.size()).reversed())
            .flatMap(Collection::stream)
            .collect(Collectors.toCollection(LinkedHashSet::new))
            .stream()
            .map(
                key -> {
                  var col = new TableColumn<Map<String, Object>, Object>(key);
                  col.setCellValueFactory(
                      param -> new ReadOnlyObjectWrapper<>(param.getValue().getOrDefault(key, "")));
                  return col;
                })
            .collect(Collectors.toList());
    if (Platform.isFxApplicationThread()) {
      getColumns().setAll(columns);
      getItems().setAll(data);
    } else
      Platform.runLater(
          () -> {
            getColumns().setAll(columns);
            getItems().setAll(data);
          });
  }
}
