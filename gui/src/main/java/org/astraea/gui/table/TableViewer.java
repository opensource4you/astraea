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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.event.EventHandler;
import javafx.scene.Node;
import javafx.scene.control.TableColumn;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.scene.input.KeyEvent;
import org.astraea.common.MapUtils;

public interface TableViewer {

  static Builder builder() {
    return new Builder();
  }

  void refresh();

  Node node();

  void data(List<Map<String, Object>> data);

  List<Map<String, Object>> data();

  List<Map<String, Object>> filteredData();

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

  class Builder {

    private List<Consumer<List<Map<String, Object>>>> dataListener = List.of();
    private List<Consumer<List<Map<String, Object>>>> filteredDataListener = List.of();
    private Predicate<Map<String, Object>> rowFilter = ignored -> true;
    private Predicate<String> columnFilter = ignored -> true;

    private Builder() {}

    public Builder dataListener(List<Consumer<List<Map<String, Object>>>> dataListener) {
      this.dataListener = dataListener;
      return this;
    }

    public Builder filteredDataListener(
        List<Consumer<List<Map<String, Object>>>> filteredDataListener) {
      this.filteredDataListener = filteredDataListener;
      return this;
    }

    public Builder rowFilter(Predicate<Map<String, Object>> rowFilter) {
      this.rowFilter = rowFilter;
      return this;
    }

    public Builder columnFilter(Predicate<String> columnFilter) {
      this.columnFilter = columnFilter;
      return this;
    }

    public TableViewer build() {
      var table = new javafx.scene.control.TableView<Map<String, Object>>();
      table.getSelectionModel().setCellSelectionEnabled(true);
      table.setOnKeyPressed(new CopyableEvent());
      var dataListener = List.copyOf(Builder.this.dataListener);
      var filteredDataListener = List.copyOf(Builder.this.filteredDataListener);
      var rowFilter = Builder.this.rowFilter;
      var columnFilter = Builder.this.columnFilter;

      return new TableViewer() {
        private volatile List<Map<String, Object>> data = List.of();
        private volatile List<Map<String, Object>> filteredData = List.of();

        @Override
        public void refresh() {
          List<Map<String, Object>> result =
              data.stream()
                  .filter(rowFilter)
                  .map(
                      item ->
                          item.entrySet().stream()
                              .filter(e -> columnFilter.test(e.getKey()))
                              .collect(
                                  MapUtils.toLinkedHashMap(Map.Entry::getKey, Map.Entry::getValue)))
                  .collect(Collectors.toUnmodifiableList());
          filteredData = result;
          filteredDataListener.forEach(c -> c.accept(filteredData));
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
                                new ReadOnlyObjectWrapper<>(
                                    param.getValue().getOrDefault(key, "")));
                        return col;
                      })
                  .collect(Collectors.toUnmodifiableList());
          if (Platform.isFxApplicationThread()) {
            table.getColumns().setAll(columns);
            table.getItems().setAll(result);
          } else
            Platform.runLater(
                () -> {
                  table.getColumns().setAll(columns);
                  table.getItems().setAll(result);
                });
        }

        @Override
        public Node node() {
          return table;
        }

        @Override
        public void data(List<Map<String, Object>> data) {
          this.data = List.copyOf(data);
          dataListener.forEach(c -> c.accept(this.data));
          refresh();
        }

        @Override
        public List<Map<String, Object>> data() {
          return data;
        }

        @Override
        public List<Map<String, Object>> filteredData() {
          return filteredData;
        }
      };
    }
  }
}
