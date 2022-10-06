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
package org.astraea.gui;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.collections.FXCollections;
import javafx.geometry.HPos;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.RadioButton;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.ToggleGroup;
import javafx.scene.input.KeyCode;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.VBox;

class Utils {
  public static TextField onlyNumber(int defaultValue) {
    var field = new TextField(String.valueOf(defaultValue));
    field
        .textProperty()
        .addListener(
            (observable, oldValue, newValue) -> {
              if (!newValue.matches("\\d*")) {
                field.setText(newValue.replaceAll("[^\\d]", ""));
              }
            });
    return field;
  }

  public static TextField copyableField(String content) {
    var field = new TextField(content);
    field.setEditable(false);
    return field;
  }

  public static Label label(String content) {
    return new Label(content);
  }

  public static HBox hbox(Node... nodes) {
    return hbox(Pos.CENTER_LEFT, nodes);
  }

  public static HBox hbox(Pos pos, Node... nodes) {
    var pane = new HBox(10);
    pane.setAlignment(pos);
    pane.getChildren().setAll(nodes);
    return pane;
  }

  public static VBox vbox(Node... nodes) {
    return vbox(Pos.TOP_LEFT, nodes);
  }

  public static VBox vbox(Pos pos, Node... nodes) {
    var pane = new VBox(10);
    pane.setPadding(new Insets(15));
    pane.getChildren().setAll(nodes);
    pane.setAlignment(pos);
    return pane;
  }

  public static <T extends Map<String, Object>> Pane searchToTable(
      BiFunction<String, Console, CompletionStage<List<T>>> itemGenerator) {
    return searchToTable(
        (word, console) -> itemGenerator.apply(word, console).thenApply(SearchResult::of),
        null,
        List.of());
  }

  public static <T extends Map<String, Object>, N extends Node> Pane searchToTable(
      BiFunction<String, Console, CompletionStage<List<T>>> itemGenerator, Collection<N> nodes) {
    return searchToTable(
        (word, console) -> itemGenerator.apply(word, console).thenApply(SearchResult::of),
        null,
        nodes);
  }

  public static <R> Pane form(
      LinkedHashSet<String> requiredFields,
      LinkedHashSet<String> optionalFields,
      BiFunction<Map<String, String>, Console, CompletionStage<R>> action,
      String buttonText) {

    var allTexts = new LinkedHashMap<String, TextField>();

    BiFunction<LinkedHashSet<String>, Integer, Pane> paneFunction =
        (fields, maxNumberOfItems) -> {
          var pane = new GridPane();
          pane.setAlignment(Pos.CENTER);
          var row = new AtomicInteger();
          var column = new AtomicInteger();
          fields.forEach(
              key -> {
                var label = new Label(key);
                GridPane.setHalignment(label, HPos.RIGHT);
                GridPane.setMargin(label, new Insets(10, 5, 10, 15));
                pane.add(label, column.getAndIncrement() % maxNumberOfItems, row.get());

                var textField = new TextField();
                GridPane.setHalignment(textField, HPos.LEFT);
                GridPane.setMargin(textField, new Insets(10, 15, 10, 5));
                pane.add(textField, column.getAndIncrement() % maxNumberOfItems, row.get());
                if (column.get() % maxNumberOfItems == 0) row.incrementAndGet();
                allTexts.put(key, textField);
              });
          return pane;
        };
    var button = new Button(buttonText);
    var console = new ConsoleArea();

    Runnable runAction =
        () -> {
          var items =
              allTexts.entrySet().stream()
                  .map(
                      entry -> {
                        var text = entry.getValue().getText();
                        if (text == null || text.isBlank())
                          return Optional.<Map.Entry<String, String>>empty();
                        return Optional.of(Map.entry(entry.getKey(), text));
                      })
                  .filter(Optional::isPresent)
                  .map(Optional::get)
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
          try {
            button.setDisable(true);
            action
                .apply(items, console)
                .whenComplete(
                    (r, e) -> {
                      try {
                        console.text(r.toString(), e);
                      } finally {
                        Platform.runLater(() -> button.setDisable(false));
                      }
                    });
          } catch (Exception e) {
            button.setDisable(false);
            console.text(e);
          }
        };

    button.setOnAction(ignored -> runAction.run());
    return Utils.vbox(
        Pos.CENTER,
        paneFunction.apply(requiredFields, 6),
        paneFunction.apply(optionalFields, 6),
        button,
        console);
  }

  public static <T, N extends Node, R> Pane searchToTable(
      BiFunction<String, Console, CompletionStage<SearchResult<T>>> resultGenerator,
      BiFunction<SearchResult<T>, Console, CompletionStage<R>> resultConsumer,
      Collection<N> nodes) {
    var view = new TableView<>(FXCollections.<Map<String, Object>>observableArrayList());
    view.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);
    var console = new ConsoleArea();
    var search = new TextField("");
    var isRunning = new AtomicBoolean(false);
    var lastResult = new AtomicReference<SearchResult<T>>();
    var applyResultButton = new Button("apply");
    var searchButton = new Button("SEARCH");

    Runnable processSearch =
        () -> {
          if (!isRunning.compareAndSet(false, true)) {
            console.append("previous search is running");
            return;
          }
          searchButton.setDisable(true);
          var value = search.getText();
          var word = value == null || value.isBlank() ? "" : value;
          applyResultButton.setVisible(false);
          console.text("search for " + (word.isEmpty() ? "all" : word));
          view.getItems().clear();
          CompletionStage<SearchResult<T>> future;
          try {
            future = resultGenerator.apply(word, console);
          } catch (Exception e) {
            isRunning.set(false);
            searchButton.setDisable(false);
            console.append(e);
            return;
          }

          future.whenComplete(
              (result, e) -> {
                if (e != null || result == null || result == SearchResult.empty()) {
                  console.append("can't generate result. Please retry it.", e);
                  searchButton.setDisable(false);
                  isRunning.set(false);
                  return;
                }
                var tables =
                    result.keys().stream()
                        .map(
                            key -> {
                              var col = new TableColumn<Map<String, Object>, Object>(key);
                              col.setCellValueFactory(
                                  param ->
                                      new ReadOnlyObjectWrapper<>(
                                          param.getValue().getOrDefault(key, "")));
                              return col;
                            })
                        .collect(Collectors.toList());
                lastResult.set(result);
                Platform.runLater(
                    () -> {
                      view.getColumns().setAll(tables);
                      view.getItems().setAll(result.items());
                      searchButton.setDisable(false);
                      isRunning.set(false);
                      // There is a callback of result, so we display the button.
                      if (resultConsumer != null) applyResultButton.setVisible(true);
                    });
              });
        };

    applyResultButton.setVisible(false);
    searchButton.setOnAction(event -> processSearch.run());
    search.setOnKeyPressed(
        event -> {
          if (event.getCode().equals(KeyCode.ENTER)) processSearch.run();
        });
    if (resultConsumer != null)
      applyResultButton.setOnAction(
          ignored -> {
            applyResultButton.setVisible(false);
            var result = lastResult.get();
            if (result == null) {
              console.append("there is no result!!!");
              return;
            }
            console.append("result is applying");
            resultConsumer
                .apply(result, console)
                .whenComplete((r, e) -> console.append("result is applied", e));
          });
    var topItems = new LinkedList<Node>(nodes);
    topItems.addLast(search);
    topItems.addLast(searchButton);
    return vbox(
        Pos.TOP_RIGHT, hbox(topItems.toArray(Node[]::new)), view, applyResultButton, console);
  }

  public static String toString(Throwable e) {
    var sw = new StringWriter();
    var pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    return sw.toString();
  }

  public static String format(long timestamp) {
    if (timestamp > 0) {
      var format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
      return format.format(new Date(timestamp));
    }
    return "unknown";
  }

  public static String formatCurrentTime() {
    return format(System.currentTimeMillis());
  }

  public static <E extends Enum<E>, T extends Enum<E>> Map<T, RadioButton> radioButton(T[] keys) {
    var group = new ToggleGroup();
    var result = new HashMap<T, RadioButton>();
    for (var i = 0; i != keys.length; ++i) {
      var button = new RadioButton(keys[i].toString());
      button.setToggleGroup(group);
      button.setSelected(i == 0);
      result.put(keys[i], button);
    }
    return result;
  }

  public static boolean contains(String source, String word) {
    if (word.isEmpty()) return true;
    return source.toLowerCase().contains(word.toLowerCase());
  }

  private Utils() {}
}
