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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.event.EventHandler;
import javafx.geometry.Side;
import javafx.scene.Node;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.scene.input.KeyEvent;
import javafx.scene.layout.BorderPane;
import org.astraea.common.MapUtils;
import org.astraea.gui.Query;
import org.astraea.gui.pane.Slide;
import org.astraea.gui.text.EditableText;

public interface TableViewer {

  int CELL_LENGTH_MAX = 70;

  Node node();

  void data(Map<String, List<Map<String, Object>>> data);

  List<Map<String, Object>> filteredData();

  /**
   * invoked by query field.
   *
   * @param keyAction the pressed key on the query field
   */
  void keyAction(Consumer<KeyEvent> keyAction);

  class CopyableEvent implements EventHandler<KeyEvent> {
    private static final List<KeyCodeCombination> COPY_CELL =
        List.of(
            new KeyCodeCombination(KeyCode.C, KeyCombination.CONTROL_DOWN),
            new KeyCodeCombination(KeyCode.C, KeyCombination.SHORTCUT_DOWN));

    private static final List<KeyCodeCombination> COPY_ALL =
        List.of(
            new KeyCodeCombination(KeyCode.A, KeyCombination.CONTROL_DOWN),
            new KeyCodeCombination(KeyCode.A, KeyCombination.SHORTCUT_DOWN));

    @Override
    public void handle(KeyEvent event) {
      if (event.getSource() instanceof javafx.scene.control.TableView) {
        var tableView = (javafx.scene.control.TableView<Map<String, Object>>) event.getSource();
        if (COPY_CELL.stream().anyMatch(c -> c.match(event))) {
          var pos = tableView.getFocusModel().getFocusedCell();
          if (pos.getRow() >= 0) {
            var item = tableView.getItems().get(pos.getRow());
            var value = pos.getTableColumn().getCellObservableValue(item).getValue();
            var clipboardContent = new ClipboardContent();
            clipboardContent.putString(value.toString());
            Clipboard.getSystemClipboard().setContent(clipboardContent);
          }
        }

        if (COPY_ALL.stream().anyMatch(c -> c.match(event))) {
          var items = List.copyOf(tableView.getItems());
          var keys =
              items.stream()
                  .flatMap(a -> a.keySet().stream())
                  .distinct()
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
      }
    }
  }

  static TableViewer disableQuery() {
    return of(false);
  }

  static TableViewer of() {
    return of(true);
  }

  private static TableViewer of(boolean enableQuery) {
    var queryField =
        EditableText.singleLine()
            .hint(
                "press ENTER to query. example: topic=chia && size>10GB || *timestamp*>=2022-10-22T04:57:43.530")
            .build();
    Supplier<Query> querySupplier =
        () -> enableQuery ? queryField.text().map(Query::of).orElse(Query.ALL) : Query.ALL;
    var totalLabel = new Label();
    var borderPane = new BorderPane();
    if (enableQuery) borderPane.setTop(queryField.node());
    borderPane.setBottom(totalLabel);
    Consumer<Integer> updateTotal =
        value -> Platform.runLater(() -> totalLabel.setText("total: " + value));
    return new TableViewer() {

      private volatile Slide<TableView<Map<String, Object>>> slide;
      private volatile Map<String, List<Map<String, Object>>> allData = Map.of();
      private volatile Map<String, List<Map<String, Object>>> allFilteredData = Map.of();

      private void refresh() {
        var query = querySupplier.get();
        allFilteredData =
            allData.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        e ->
                            e.getValue().stream()
                                .filter(query::required)
                                .map(
                                    item -> {
                                      var requiredKeys = new LinkedHashSet<String>();
                                      item.keySet().stream()
                                          .findFirst()
                                          .ifPresent(requiredKeys::add);
                                      item.keySet().stream()
                                          .skip(0)
                                          .filter(query::required)
                                          .forEach(requiredKeys::add);
                                      item.keySet().stream()
                                          .filter(k -> !query.required(k))
                                          .forEach(requiredKeys::add);
                                      return requiredKeys.stream()
                                          .collect(MapUtils.toLinkedHashMap(k -> k, item::get));
                                    })
                                .collect(Collectors.toUnmodifiableList())));

        var allTables =
            allData.keySet().stream()
                .collect(
                    Collectors.toMap(
                        Function.identity(),
                        name -> {
                          var table = new TableView<Map<String, Object>>();
                          table.getSelectionModel().setCellSelectionEnabled(true);
                          table.setOnKeyPressed(new CopyableEvent());
                          var filteredData = allFilteredData.get(name);
                          var sortName =
                              table.getSortOrder().isEmpty()
                                  ? filteredData.isEmpty()
                                      ? null
                                      : filteredData.get(0).entrySet().iterator().next().getKey()
                                  : table.getSortOrder().get(0).getText();
                          var sortType =
                              table.getSortOrder().isEmpty()
                                  ? TableColumn.SortType.ASCENDING
                                  : table.getSortOrder().get(0).getSortType();
                          var columns =
                              filteredData.stream()
                                  .map(Map::keySet)
                                  .sorted(
                                      Comparator.comparingInt((Set<String> o) -> o.size())
                                          .reversed())
                                  .flatMap(Collection::stream)
                                  .collect(Collectors.toCollection(LinkedHashSet::new))
                                  .stream()
                                  .map(
                                      key -> {
                                        var col = new TableColumn<Map<String, Object>, Object>(key);
                                        col.setCellValueFactory(
                                            param -> {
                                              var obj = param.getValue().get(key);
                                              if (obj instanceof String)
                                                return new ReadOnlyObjectWrapper<>(
                                                    String.join(
                                                        "\n",
                                                        TableViewer.chunk(
                                                            (String) obj, CELL_LENGTH_MAX)));

                                              return new ReadOnlyObjectWrapper<>(
                                                  obj == null ? "" : obj);
                                            });
                                        return col;
                                      })
                                  .collect(Collectors.toUnmodifiableList());
                          Runnable updater =
                              () -> {
                                table.getColumns().setAll(columns);
                                table.getItems().setAll(filteredData);
                                columns.stream()
                                    .filter(c -> sortName != null && c.getText().equals(sortName))
                                    .findFirst()
                                    .ifPresent(
                                        c -> {
                                          table.getSortOrder().add(c);
                                          c.setSortType(sortType);
                                          c.setSortable(true);
                                        });
                              };
                          if (Platform.isFxApplicationThread()) updater.run();
                          else Platform.runLater(updater);
                          return table;
                        }));
        slide =
            Slide.of(
                Side.BOTTOM,
                allTables,
                (name, t) -> updateTotal.accept(allFilteredData.get(name).size()));
        Platform.runLater(() -> borderPane.setCenter(slide.node()));
      }

      @Override
      public Node node() {
        return borderPane;
      }

      @Override
      public void data(Map<String, List<Map<String, Object>>> data) {
        this.allData = data;
        refresh();
      }

      @Override
      public List<Map<String, Object>> filteredData() {
        var current = slide;
        if (current == null) return List.of();
        var selected = current.selected();
        var filteredData = allFilteredData.get(selected.getKey());
        if (filteredData == null) return List.of();
        return filteredData;
      }

      @Override
      public void keyAction(Consumer<KeyEvent> keyAction) {
        if (enableQuery) queryField.keyAction(keyAction);
      }
    };
  }

  private static List<String> chunk(String s, int size) {
    if (s.length() <= size) return List.of(s);
    var result = new ArrayList<String>();
    var current = "";
    for (var i : s.split(" ")) {
      current = current + " " + i;
      if (current.length() >= size) {
        result.add(current);
        current = "";
      }
    }
    if (!current.isBlank()) result.add(current);
    return result;
  }
}
