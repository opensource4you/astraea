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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.input.KeyCode;
import javafx.scene.layout.Pane;
import org.astraea.common.MapUtils;
import org.astraea.gui.Logger;
import org.astraea.gui.box.HBox;
import org.astraea.gui.box.VBox;
import org.astraea.gui.button.Button;
import org.astraea.gui.button.RadioButton;
import org.astraea.gui.table.TableView;
import org.astraea.gui.text.Label;
import org.astraea.gui.text.TextArea;
import org.astraea.gui.text.TextField;

/** a template layout for all tabs. */
public class PaneBuilder {

  public static PaneBuilder of() {
    return new PaneBuilder();
  }

  private List<RadioButton> radioButtons = new ArrayList<>();

  private final Set<String> inputKeys = new LinkedHashSet<>();

  private final Map<String, Boolean> inputKeyPlaceholder = new HashMap<>();
  private final Map<String, String> inputKeyValue = new HashMap<>();

  private final Map<String, Boolean> inputKeyPriority = new LinkedHashMap<>();
  private final Map<String, Boolean> inputKeyNumberOnly = new LinkedHashMap<>();
  private Label searchLabel = null;
  private final TextField searchField = TextField.of();

  private Button actionButton = Button.of("SEARCH");

  private TableView tableView = null;

  private final TextArea console = TextArea.of();

  private BiFunction<Input, Logger, CompletionStage<List<Map<String, Object>>>> buttonAction = null;
  private BiFunction<Input, Logger, CompletionStage<Void>> buttonListener = null;

  private PaneBuilder() {}

  public PaneBuilder singleRadioButtons(Object[] objs) {
    return singleRadioButtons(Arrays.asList(objs));
  }

  public PaneBuilder singleRadioButtons(List<Object> objs) {
    if (objs.isEmpty()) return this;
    radioButtons = RadioButton.single(objs);
    return this;
  }

  public PaneBuilder multiRadioButtons(Object[] objs) {
    return multiRadioButtons(Arrays.asList(objs));
  }

  public PaneBuilder multiRadioButtons(List<Object> objs) {
    if (objs.isEmpty()) return this;
    radioButtons = RadioButton.multi(objs);
    return this;
  }

  /**
   * add key to this builder
   *
   * @param key to add
   * @param required true if key is required. The font will get highlight
   * @param numberOnly true if the value associated to key must be number
   * @return this builder
   */
  public PaneBuilder input(String key, boolean required, boolean numberOnly) {
    return input(key, required, numberOnly, false, null);
  }

  /**
   * add key to this builder
   *
   * @param key to add
   * @param required true if key is required. The font will get highlight
   * @param numberOnly true if the value associated to key must be number
   * @param usePlaceholder true if the default value is treated as placeholder
   * @param defaultValue to show by default
   * @return this builder
   */
  public PaneBuilder input(
      String key,
      boolean required,
      boolean numberOnly,
      boolean usePlaceholder,
      String defaultValue) {
    inputKeys.add(key);
    inputKeyPriority.put(key, required);
    inputKeyNumberOnly.put(key, numberOnly);
    inputKeyPlaceholder.put(key, usePlaceholder);
    if (defaultValue != null)
      inputKeyValue.put(
          key, numberOnly ? String.valueOf(Long.parseLong(defaultValue)) : defaultValue);

    return this;
  }

  /**
   * add optional keys to this builder.
   *
   * @param keys optional keys
   * @return this builder
   */
  public PaneBuilder input(Set<String> keys) {
    keys.forEach(k -> input(k, false, false));
    return this;
  }

  /**
   * set the keys and their default values
   *
   * @param keysAndValues keys and default values
   * @return this
   */
  public PaneBuilder input(Map<String, String> keysAndValues) {
    keysAndValues.forEach((k, v) -> input(k, false, false, false, v));
    return this;
  }

  public PaneBuilder searchField(String hint) {
    searchLabel = Label.of(hint);
    return this;
  }

  public PaneBuilder buttonName(String name) {
    actionButton = Button.of(name);
    return this;
  }

  public PaneBuilder buttonAction(
      BiFunction<Input, Logger, CompletionStage<List<Map<String, Object>>>> buttonAction) {
    this.buttonAction = buttonAction;
    if (tableView == null) tableView = TableView.copyable();
    return this;
  }

  public PaneBuilder buttonListener(
      BiFunction<Input, Logger, CompletionStage<Void>> buttonListener) {
    this.buttonListener = buttonListener;
    return this;
  }

  public PaneBuilder initTableView(List<Map<String, Object>> data) {
    if (tableView == null) tableView = TableView.copyable();
    tableView.update(data);
    return this;
  }

  public Pane build() {
    var nodes = new ArrayList<Node>();
    if (!radioButtons.isEmpty()) nodes.add(HBox.of(Pos.CENTER, radioButtons.toArray(Node[]::new)));
    Map<String, Supplier<String>> textFields;
    if (!inputKeys.isEmpty()) {
      var pairs =
          inputKeys.stream()
              .collect(
                  MapUtils.toLinkedHashMap(
                      key ->
                          inputKeyPriority.getOrDefault(key, false)
                              ? Label.highlight(key)
                              : Label.of(key),
                      key -> {
                        var builder = TextField.builder();
                        if (inputKeyNumberOnly.getOrDefault(key, false))
                          builder = builder.onlyNumber();
                        String value = inputKeyValue.get(key);
                        if (value != null) {
                          if (inputKeyPlaceholder.getOrDefault(key, false))
                            builder = builder.placeholder(value);
                          else builder = builder.defaultValue(value);
                        }
                        if (inputKeyNumberOnly.getOrDefault(key, false))
                          builder = builder.onlyNumber();
                        return builder.build();
                      }));
      var gridPane = pairs.size() <= 3 ? GridPane.singleColumn(pairs, 3) : GridPane.of(pairs, 3);
      nodes.add(gridPane);
      textFields =
          pairs.entrySet().stream()
              .collect(
                  MapUtils.toLinkedHashMap(
                      e -> e.getKey().key(), e -> () -> e.getValue().getText()));
    } else textFields = Map.of();
    if (searchLabel != null) nodes.add(HBox.of(Pos.CENTER, searchLabel, searchField, actionButton));
    else nodes.add(actionButton);
    if (tableView != null) nodes.add(tableView);
    nodes.add(console);

    Runnable handler =
        () -> {
          var singleSelectedRadio =
              radioButtons.stream()
                  .filter(RadioButton::isSelected)
                  .flatMap(r -> r.selectedObject().stream())
                  .findFirst();
          var multiSelectedRadio =
              radioButtons.stream()
                  .filter(RadioButton::isSelected)
                  .flatMap(r -> r.selectedObject().stream())
                  .collect(Collectors.toList());
          var rawTexts =
              textFields.entrySet().stream()
                  .flatMap(
                      entry ->
                          Optional.ofNullable(entry.getValue().get())
                              .map(v -> Map.entry(entry.getKey(), v))
                              .stream())
                  .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
          var requiredNonexistentKeys =
              inputKeyPriority.entrySet().stream()
                  .filter(
                      entry ->
                          entry.getValue()
                              && (!rawTexts.containsKey(entry.getKey())
                                  || rawTexts.get(entry.getKey()).isBlank()))
                  .map(Map.Entry::getKey)
                  .collect(Collectors.toSet());
          if (!requiredNonexistentKeys.isEmpty()) {
            console.text("Please define required fields: " + requiredNonexistentKeys);
            return;
          }

          var searchPatterns =
              Arrays.stream(searchField.getText().split(","))
                  .filter(s -> !s.isBlank())
                  .map(PaneBuilder::wildcardToPattern)
                  .collect(Collectors.toList());
          Logger logger = console::append;
          var input =
              new Input() {
                @Override
                @SuppressWarnings("unchecked")
                public <T> T singleSelectedRadio(T defalutObj) {
                  return (T) singleSelectedRadio.orElse(defalutObj);
                }

                @Override
                @SuppressWarnings("unchecked")
                public <T> List<T> multiSelectedRadios(List<T> df) {
                  return multiSelectedRadio.isEmpty() ? df : (List<T>) multiSelectedRadio;
                }

                @Override
                public Map<String, String> texts() {
                  return rawTexts;
                }

                @Override
                public boolean matchSearch(String word) {
                  return searchPatterns.isEmpty()
                      || searchPatterns.stream().anyMatch(p -> p.matcher(word).matches());
                }
              };

          // nothing to do
          if (buttonAction == null && buttonListener == null) return;

          console.cleanup();
          actionButton.disable();
          try {
            if (buttonAction != null)
              buttonAction
                  .apply(input, logger)
                  .whenComplete(
                      (data, e) -> {
                        try {
                          if (data != null) tableView.update(data);
                          console.text(e);
                        } finally {
                          actionButton.enable();
                        }
                      });
            if (buttonListener != null)
              buttonListener
                  .apply(input, logger)
                  .whenComplete(
                      (data, e) -> {
                        try {
                          console.text(e);
                        } finally {
                          actionButton.enable();
                        }
                      });

          } catch (IllegalArgumentException e) {
            console.append(e.getMessage());
            actionButton.enable();
          } catch (Exception e) {
            console.append(e);
            actionButton.enable();
          }
        };

    actionButton.setOnAction(ignored -> handler.run());
    // there is only one text field, so we register the ENTER event.
    if (textFields.isEmpty())
      searchField.setOnKeyPressed(
          event -> {
            if (event.getCode().equals(KeyCode.ENTER)) handler.run();
          });

    return VBox.of(Pos.CENTER, nodes.toArray(Node[]::new));
  }

  static Pattern wildcardToPattern(String string) {
    return Pattern.compile(
        string.replaceAll("\\?", ".").replaceAll("\\*", ".*"), Pattern.CASE_INSENSITIVE);
  }
}
