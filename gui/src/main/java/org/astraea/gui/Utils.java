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
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.VBox;

class Utils {
  private static final Duration DELAY_INPUT = Duration.ofMillis(600);

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

  public static <T> List<T> random(List<T> all, int number) {
    var sub = all.size() - number;
    if (sub < 0)
      throw new IllegalArgumentException(
          "only " + all.size() + " elements, but required number is " + number);
    if (sub == 0) return all;
    var result = new LinkedList<>(all);
    Collections.shuffle(result);
    return result.stream().skip(sub).collect(Collectors.toUnmodifiableList());
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
    var pane = new VBox(10);
    pane.setPadding(new Insets(15));
    pane.getChildren().setAll(nodes);
    return pane;
  }

  public static <T extends Map<String, String>> Pane searchToTable(
      String hint, Function<String, List<T>> itemGenerator) {
    return searchToTable(
        hint,
        itemGenerator,
        (ignored, e) -> {
          if (e != null) e.printStackTrace();
        });
  }

  public static <T extends Map<String, String>> Pane searchToTable(
      String hint,
      Function<String, List<T>> itemGenerator,
      BiConsumer<List<T>, Throwable> callback) {
    var view = new TableView<>(FXCollections.<Map<String, String>>observableArrayList());
    view.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);
    var search = new TextField("");
    var keyword = new AtomicReference<String>();
    search
        .textProperty()
        .addListener(
            ((observable, oldValue, newValue) -> {
              if (newValue == null) return;
              var word = newValue.isBlank() ? "" : newValue;
              // the process is already running if keyword is not null
              if (keyword.getAndSet(word) != null) return;
              view.getItems().clear();
              CompletableFuture.supplyAsync(
                      () -> itemGenerator.apply(keyword.get()),
                      CompletableFuture.delayedExecutor(
                          DELAY_INPUT.toMillis(), TimeUnit.MILLISECONDS))
                  .whenComplete(
                      (items, e) -> {
                        if (items == null) {
                          keyword.set(null);
                          callback.accept(null, e);
                          return;
                        }
                        callback.accept(items, e);
                        var keys =
                            items.stream()
                                .flatMap(i -> i.keySet().stream())
                                .collect(Collectors.toCollection(LinkedHashSet::new));
                        var tables =
                            keys.stream()
                                .map(
                                    key -> {
                                      var col = new TableColumn<Map<String, String>, String>(key);
                                      col.setCellValueFactory(
                                          param ->
                                              new ReadOnlyObjectWrapper<>(
                                                  param.getValue().getOrDefault(key, "")));
                                      return col;
                                    })
                                .collect(Collectors.toList());
                        Platform.runLater(
                            () -> {
                              view.getColumns().setAll(tables);
                              view.getItems().setAll(items);
                              keyword.set(null);
                            });
                      });
            }));
    var pane = new BorderPane();
    pane.setPadding(new Insets(10, 20, 10, 20));
    pane.setTop(hbox(new Label(hint), search));
    pane.setCenter(view);
    return pane;
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

  private Utils() {}
}
