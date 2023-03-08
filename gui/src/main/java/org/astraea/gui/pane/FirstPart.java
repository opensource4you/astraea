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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import org.astraea.gui.Logger;
import org.astraea.gui.button.SelectBox;
import org.astraea.gui.text.TextInput;

public class FirstPart {

  public static Builder builder() {
    return new Builder();
  }

  private final Optional<SelectBox> selectBox;
  private final List<TextInput> textInputs;
  private final String clickName;
  private final BiFunction<
          Argument, Logger, CompletionStage<Map<String, List<Map<String, Object>>>>>
      tableRefresher;
  private final BiFunction<Argument, Logger, CompletionStage<Map<String, String>>> tipRefresher;

  public FirstPart(
      Optional<SelectBox> selectBox,
      List<TextInput> textInputs,
      String clickName,
      BiFunction<Argument, Logger, CompletionStage<Map<String, List<Map<String, Object>>>>>
          tableRefresher,
      BiFunction<Argument, Logger, CompletionStage<Map<String, String>>> tipRefresher) {
    this.selectBox = selectBox;
    this.textInputs = textInputs;
    this.clickName = clickName;
    this.tableRefresher = tableRefresher;
    this.tipRefresher = tipRefresher;
  }

  public Optional<SelectBox> selectBox() {
    return selectBox;
  }

  public List<TextInput> textInputs() {
    return textInputs;
  }

  public String clickName() {
    return clickName;
  }

  public BiFunction<Argument, Logger, CompletionStage<Map<String, List<Map<String, Object>>>>>
      tableRefresher() {
    return tableRefresher;
  }

  public BiFunction<Argument, Logger, CompletionStage<Map<String, String>>> tipRefresher() {
    return tipRefresher;
  }

  public static class Builder {
    private Builder() {}

    private Optional<SelectBox> selectBox = Optional.empty();
    private List<TextInput> textInputs = List.of();
    private String clickName = "click";
    private BiFunction<Argument, Logger, CompletionStage<Map<String, List<Map<String, Object>>>>>
        tablesRefresher = (i, y) -> CompletableFuture.completedFuture(Map.of());
    private BiFunction<Argument, Logger, CompletionStage<Map<String, String>>> tipRefresher =
        (i, y) -> CompletableFuture.completedFuture(Map.of());

    public Builder selectBox(SelectBox selectBox) {
      this.selectBox = Optional.of(selectBox);
      return this;
    }

    public Builder textInputs(List<TextInput> textInputs) {
      this.textInputs = textInputs;
      return this;
    }

    public Builder clickName(String clickName) {
      this.clickName = clickName;
      return this;
    }

    public Builder tableRefresher(
        BiFunction<Argument, Logger, CompletionStage<List<Map<String, Object>>>> tableRefresher) {
      return tablesRefresher(
          (argument, logger) ->
              tableRefresher
                  .apply(argument, logger)
                  .thenApply(r -> r.isEmpty() ? Map.of() : Map.of("basic", r)));
    }

    public Builder tablesRefresher(
        BiFunction<Argument, Logger, CompletionStage<Map<String, List<Map<String, Object>>>>>
            tablesRefresher) {
      this.tablesRefresher = tablesRefresher;
      return this;
    }

    public Builder tipRefresher(
        BiFunction<Argument, Logger, CompletionStage<Map<String, String>>> tipRefresher) {
      this.tipRefresher = tipRefresher;
      return this;
    }

    public FirstPart build() {
      return new FirstPart(selectBox, textInputs, clickName, tablesRefresher, tipRefresher);
    }
  }
}
