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
import javafx.scene.Node;
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
import org.astraea.gui.text.EditableText;

public interface TableViewer {

  void refresh();

  Node node();

  void data(List<Map<String, Object>> data);

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

  static TableViewer of() {
    var table = new TableView<Map<String, Object>>();
    table.getSelectionModel().setCellSelectionEnabled(true);
    table.setOnKeyPressed(new CopyableEvent());
    var queryField =
        EditableText.singleLine()
            .hint(
                "press ENTER to query. example: topic=chia && size>10GB || *timestamp*>=2022-10-22T04:57:43.530")
            .build();
    Supplier<Query> querySupplier = () -> queryField.text().map(Query::of).orElse(Query.ALL);
    var borderPane = new BorderPane();
    borderPane.setTop(queryField.node());
    borderPane.setCenter(table);
    Function<List<Map<String, Object>>, Set<String>> toKey =
        data ->
            data.stream()
                .map(Map::keySet)
                .sorted(Comparator.comparingInt((Set<String> o) -> o.size()).reversed())
                .flatMap(Collection::stream)
                .collect(Collectors.toCollection(LinkedHashSet::new));

    return new TableViewer() {
      private volatile List<Map<String, Object>> data = List.of();
      private volatile List<Map<String, Object>> filteredData = List.of();

      @Override
      public void refresh() {
        var query = querySupplier.get();
        List<Map<String, Object>> result =
            data.stream()
                .filter(query::required)
                .map(
                    item -> {
                      var requiredKeys = new LinkedHashSet<String>();
                      item.keySet().stream().findFirst().ifPresent(requiredKeys::add);
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
                .collect(Collectors.toUnmodifiableList());
        filteredData = result;
        var sortName =
            table.getSortOrder().isEmpty()
                ? result.isEmpty() ? null : result.get(0).entrySet().iterator().next().getKey()
                : table.getSortOrder().get(0).getText();
        var sortType =
            table.getSortOrder().isEmpty()
                ? TableColumn.SortType.ASCENDING
                : table.getSortOrder().get(0).getSortType();
        var columns =
            result.stream()
                .map(Map::keySet)
                .sorted(Comparator.comparingInt((Set<String> o) -> o.size()).reversed())
                .flatMap(Collection::stream)
                .collect(Collectors.toCollection(LinkedHashSet::new))
                .stream()
                .map(
                    key -> {
                      var col = new TableColumn<Map<String, Object>, Object>(key);
                      col.setCellValueFactory(
                          param ->
                              new ReadOnlyObjectWrapper<>(param.getValue().getOrDefault(key, "")));
                      return col;
                    })
                .collect(Collectors.toUnmodifiableList());
        Runnable updater =
            () -> {
              table.getColumns().setAll(columns);
              table.getItems().setAll(result);
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
      }

      @Override
      public Node node() {
        return borderPane;
      }

      @Override
      public void data(List<Map<String, Object>> data) {
        this.data = List.copyOf(data);
        refresh();
      }

      @Override
      public List<Map<String, Object>> filteredData() {
        return filteredData;
      }

      @Override
      public void keyAction(Consumer<KeyEvent> keyAction) {
        queryField.keyAction(keyAction);
      }
    };
  }
}
