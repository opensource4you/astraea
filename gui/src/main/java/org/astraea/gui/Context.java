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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.collections.FXCollections;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.Pane;
import org.astraea.common.admin.Admin;

public class Context {
  private final AtomicReference<Admin> atomicReference = new AtomicReference<>();

  public Optional<Admin> replace(Admin admin) {
    return Optional.ofNullable(atomicReference.getAndSet(admin));
  }

  public Optional<Admin> optionalAdmin() {
    return Optional.ofNullable(atomicReference.get());
  }

  <T> Pane tableView(BiFunction<Admin, String, Result<T>> resultGenerator) {
    var view = new TableView<>(FXCollections.<T>observableArrayList());
    view.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);
    var search = new TextField("enter to search");
    search
        .textProperty()
        .addListener(
            ((observable, oldValue, newValue) -> {
              if (newValue == null) return;
              if (newValue.equals(oldValue)) return;
              optionalAdmin()
                  .ifPresent(
                      admin ->
                          CompletableFuture.supplyAsync(
                                  () -> resultGenerator.apply(admin, newValue))
                              .whenComplete(
                                  (result, e) -> {
                                    if (result == null) return;
                                    Platform.runLater(
                                        () -> {
                                          view.getColumns().setAll(result.columns());
                                          view.getItems().setAll(result.values());
                                        });
                                  }));
            }));
    var pane = new BorderPane();
    pane.setTop(search);
    pane.setCenter(view);
    return pane;
  }

  public static <T> Result<T> result(
      Map<String, Function<T, Object>> columnGetter, List<T> values) {
    return result(
        columnGetter.entrySet().stream()
            .map(
                entry -> {
                  var col = new TableColumn<T, Object>(entry.getKey());
                  col.setCellValueFactory(
                      param ->
                          new ReadOnlyObjectWrapper<>(entry.getValue().apply(param.getValue())));
                  return col;
                })
            .collect(Collectors.toList()),
        values);
  }

  static <T> Result<T> result(List<TableColumn<T, Object>> columns, List<T> values) {
    return new Result<>() {
      @Override
      public List<TableColumn<T, Object>> columns() {
        return columns;
      }

      @Override
      public List<T> values() {
        return values;
      }
    };
  }

  interface Result<T> {
    List<TableColumn<T, Object>> columns();

    List<T> values();
  }
}
