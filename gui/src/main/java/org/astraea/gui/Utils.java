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
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.collections.FXCollections;
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
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.VBox;

class Utils {
  private static final Duration DELAY_INPUT = Duration.ofMillis(900);

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
      BiFunction<String, Console, List<T>> itemGenerator) {
    return searchToTable(
        (word, console) -> SearchResult.of(itemGenerator.apply(word, console)), null, List.of());
  }

  public static <T extends Map<String, Object>, N extends Node> Pane searchToTable(
      BiFunction<String, Console, List<T>> itemGenerator, Collection<N> nodes) {
    return searchToTable(
        (word, console) -> SearchResult.of(itemGenerator.apply(word, console)), null, nodes);
  }

  public static <T, N extends Node> Pane searchToTable(
      BiFunction<String, Console, SearchResult<T>> resultGenerator,
      BiConsumer<SearchResult<T>, Console> resultConsumer,
      Collection<N> nodes) {
    var view = new TableView<>(FXCollections.<Map<String, Object>>observableArrayList());
    view.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);
    var console = new ConsoleArea();
    var search = new TextField("");
    var isRunning = new AtomicBoolean(false);
    var lastResult = new AtomicReference<SearchResult<T>>();
    var applyResultButton = new Button("apply");
    var searchButton = new Button("search");

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
          CompletableFuture.supplyAsync(
                  () -> resultGenerator.apply(word, console),
                  CompletableFuture.delayedExecutor(DELAY_INPUT.toMillis(), TimeUnit.MILLISECONDS))
              .whenComplete(
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
            console.append("Applying result ... ");
            CompletableFuture.runAsync(() -> resultConsumer.accept(result, console))
                .whenComplete((r, e) -> console.append(e));
          });

    return vbox(
        Pos.TOP_RIGHT,
        hbox(Stream.concat(Stream.of(search, searchButton), nodes.stream()).toArray(Node[]::new)),
        view,
        applyResultButton,
        console);
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
      var button = new RadioButton(keys[i].name());
      button.setToggleGroup(group);
      button.setSelected(i == 0);
      result.put(keys[i], button);
    }
    return result;
  }

  private Utils() {}
}
