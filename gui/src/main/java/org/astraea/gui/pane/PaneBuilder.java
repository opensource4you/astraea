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

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.collections.ListChangeListener;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.CheckBox;
import javafx.scene.input.KeyCode;
import javafx.scene.layout.Pane;
import org.astraea.common.DataSize;
import org.astraea.common.MapUtils;
import org.astraea.common.Utils;
import org.astraea.common.function.Bi3Function;
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

  // ---------------------------------[first control]---------------------------------//

  private List<RadioButton> radioButtons = new ArrayList<>();

  private final Map<Label, TextField> inputKeyAndFields = new LinkedHashMap<>();

  private Button actionButton = Button.of("SEARCH");

  private TableView tableView = null;

  private BiFunction<Input, Logger, CompletionStage<List<Map<String, Object>>>> buttonAction = null;
  private BiFunction<Input, Logger, CompletionStage<Void>> buttonListener = null;

  // ---------------------------------[second control]---------------------------------//

  private final Map<Label, TextField> secondInputKeyAndFields = new LinkedHashMap<>();

  private Button tableViewActionButton = Button.disabled("EXECUTE");

  private Bi3Function<List<Map<String, Object>>, Map<String, String>, Logger, CompletionStage<Void>>
      tableViewAction = null;

  private PaneBuilder() {}

  public PaneBuilder singleRadioButtons(Object[] objs) {
    return singleRadioButtons(Arrays.asList(objs));
  }

  public PaneBuilder singleRadioButtons(List<Object> objs) {
    if (objs.isEmpty()) return this;
    radioButtons = RadioButton.single(objs);
    return this;
  }

  public PaneBuilder multiRadioButtons(List<Object> objs) {
    if (objs.isEmpty()) return this;
    radioButtons = RadioButton.multi(objs);
    return this;
  }

  public PaneBuilder input(Label key, TextField value) {
    inputKeyAndFields.put(key, value);
    return this;
  }

  public PaneBuilder input(Map<Label, TextField> inputs) {
    inputKeyAndFields.putAll(inputs);
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

  public PaneBuilder tableViewAction(
      Map<Label, TextField> inputs,
      String buttonName,
      Bi3Function<List<Map<String, Object>>, Map<String, String>, Logger, CompletionStage<Void>>
          action) {
    secondInputKeyAndFields.putAll(inputs);
    tableViewActionButton = Button.disabled(buttonName);
    tableViewAction = action;
    return this;
  }

  public Pane build() {
    // step.1 layout
    var nodes = new ArrayList<Node>();
    if (!radioButtons.isEmpty()) nodes.add(HBox.of(Pos.CENTER, radioButtons.toArray(Node[]::new)));
    if (!inputKeyAndFields.isEmpty()) {
      var gridPane =
          inputKeyAndFields.size() <= 3
              ? GridPane.singleColumn(inputKeyAndFields)
              : GridPane.of(inputKeyAndFields, 3);
      nodes.add(gridPane);
    }
    nodes.add(actionButton);
    var console = TextArea.of();
    Logger logger = console::append;
    if (tableView != null) {
      var filterText =
          TextField.builder()
              .hint("c0,c1,c2,c3,c0=aa,c1<20,c2>30MB,c3>=2022-10-22T04:57:43.530")
              .build();
      filterText.setOnKeyPressed(
          event -> {
            if (event.getCode().equals(KeyCode.ENTER)) {
              try {
                tableView.convert(filterText.text().map(PaneBuilder::converter).orElse(v -> v));
              } catch (Exception e) {
                console.text(e);
              }
            }
          });
      var sizeLabel = Label.of("");
      tableView
          .getItems()
          .addListener(
              (ListChangeListener<Map<String, Object>>)
                  c -> sizeLabel.text("total: " + tableView.size()));
      nodes.add(VBox.of(Pos.CENTER, filterText, tableView, sizeLabel));
    }

    // ---------------------------------[second control layout]---------------------------------//
    if (tableView != null && tableViewAction != null) {
      var checkbox = new CheckBox("enable");
      checkbox
          .selectedProperty()
          .addListener(
              (observable, oldValue, newValue) -> {
                if (checkbox.isSelected()) {
                  tableViewActionButton.enable();
                  secondInputKeyAndFields.values().forEach(TextField::enable);
                } else {
                  tableViewActionButton.disable();
                  secondInputKeyAndFields.values().forEach(TextField::disable);
                }
              });
      tableViewActionButton.setOnAction(
          event -> {
            var items = tableView.items();
            var input =
                secondInputKeyAndFields.entrySet().stream()
                    .flatMap(e -> e.getValue().text().stream().map(v -> Map.entry(e.getKey(), v)))
                    .collect(Collectors.toMap(e -> e.getKey().key(), Map.Entry::getValue));
            try {
              checkbox.setSelected(false);
              tableViewAction
                  .apply(items, input, logger)
                  .whenComplete((data, e) -> console.text(e));
            } catch (Exception e) {
              console.text(e);
            }
          });

      nodes.add(
          VBox.of(
              Pos.CENTER,
              checkbox,
              GridPane.singleColumn(secondInputKeyAndFields),
              tableViewActionButton));
    }

    nodes.add(console);

    // step.2 event
    Runnable handler =
        () -> {
          var multiSelectedRadio =
              radioButtons.stream()
                  .filter(RadioButton::isSelected)
                  .flatMap(r -> r.selectedObject().stream())
                  .collect(Collectors.toList());
          var requiredNonexistentKeys =
              inputKeyAndFields.entrySet().stream()
                  .filter(entry -> entry.getKey().highlight())
                  .filter(entry -> entry.getValue().text().isEmpty())
                  .map(e -> e.getKey().key())
                  .collect(Collectors.toSet());
          if (!requiredNonexistentKeys.isEmpty()) {
            console.text("Please define required fields: " + requiredNonexistentKeys);
            return;
          }

          var rawTexts =
              inputKeyAndFields.entrySet().stream()
                  .collect(Collectors.toMap(e -> e.getKey().key(), e -> e.getValue().text()));
          var input =
              new Input() {
                @Override
                @SuppressWarnings("unchecked")
                public <T> List<T> multiSelectedRadios(List<T> defaultObjs) {
                  return multiSelectedRadio.isEmpty() ? defaultObjs : (List<T>) multiSelectedRadio;
                }

                @Override
                public Map<String, Optional<String>> texts() {
                  return rawTexts;
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
                          if (data != null) tableView.set(data);
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

          } catch (Exception e) {
            console.text(e);
            actionButton.enable();
          }
        };

    actionButton.setOnAction(ignored -> handler.run());
    return VBox.of(Pos.CENTER, nodes.toArray(Node[]::new));
  }

  /**
   * the form of text is <key>,...<key=value>. 1) if there is only key, it means the visible column
   * names 2) if there is a pair of key and value, it is the condition used to select row. For
   * example: *policy*=compact. Noted that the conditions are combined by OR rather AND. Hence, a
   * row get selected if it matches one of condition.
   *
   * @param text show the wildcard rules
   * @return predicate
   */
  private static Function<Map<String, Object>, Map<String, Object>> converter(String text) {
    if (text == null || text.isBlank()) return v -> v;

    var predicates =
        Stream.concat(forString(text).stream(), forNumber(text).stream())
            .collect(Collectors.toList());
    var visibleKeys =
        Arrays.stream(text.split(","))
            .filter(item -> !item.contains("=") && !item.contains("<") && !item.contains(">"))
            .map(Utils::wildcardToPattern)
            .collect(Collectors.toList());

    return item -> {
      if (item.isEmpty()) return item;
      var match = predicates.isEmpty() || predicates.stream().anyMatch(p -> p.test(item));
      if (!match) return Map.of();
      if (visibleKeys.isEmpty()) return item;
      var firstKey = item.entrySet().iterator().next().getKey();
      return item.entrySet().stream()
          .filter(
              e ->
                  e.getKey().equals(firstKey)
                      || visibleKeys.stream().anyMatch(p -> p.matcher(e.getKey()).matches()))
          .collect(MapUtils.toLinkedHashMap(Map.Entry::getKey, Map.Entry::getValue));
    };
  }

  private static Optional<Predicate<Map<String, Object>>> forNumber(String text) {
    var larges = new HashMap<Pattern, String>();
    var equals = new HashMap<Pattern, String>();
    var smalls = new HashMap<Pattern, String>();
    for (var item : text.split(",")) {
      var ss = item.trim().split(">=");
      if (ss.length == 2) {
        larges.put(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim());
        equals.put(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim());
        continue;
      }
      ss = item.trim().split("<=");
      if (ss.length == 2) {
        smalls.put(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim());
        equals.put(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim());
        continue;
      }
      ss = item.split("==");
      if (ss.length == 2) {
        equals.put(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim());
        continue;
      }
      ss = item.trim().split(">");
      if (ss.length == 2) {
        larges.put(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim());
        continue;
      }
      ss = item.split("<");
      if (ss.length == 2) smalls.put(Utils.wildcardToPattern(ss[0].trim()), ss[1].trim());
    }
    if (larges.isEmpty() && equals.isEmpty() && smalls.isEmpty()) return Optional.empty();
    return Optional.of(
        item ->
            item.entrySet().stream()
                .anyMatch(
                    e -> {
                      if (e.getValue() instanceof Number) {
                        var value = ((Number) e.getValue()).longValue();
                        if (larges.entrySet().stream()
                            .anyMatch(
                                large ->
                                    large.getKey().matcher(e.getKey()).matches()
                                        && value > Long.parseLong(large.getValue()))) return true;
                        if (equals.entrySet().stream()
                            .anyMatch(
                                equal ->
                                    equal.getKey().matcher(e.getKey()).matches()
                                        && value == Long.parseLong(equal.getValue()))) return true;
                        if (smalls.entrySet().stream()
                            .anyMatch(
                                small ->
                                    small.getKey().matcher(e.getKey()).matches()
                                        && value < Long.parseLong(small.getValue()))) return true;
                      }

                      if (e.getValue() instanceof DataSize) {
                        var size = ((DataSize) e.getValue());
                        if (larges.entrySet().stream()
                            .anyMatch(
                                large ->
                                    large.getKey().matcher(e.getKey()).matches()
                                        && size.greaterThan(
                                            new DataSize.Field().convert(large.getValue()))))
                          return true;
                        if (equals.entrySet().stream()
                            .anyMatch(
                                equal ->
                                    equal.getKey().matcher(e.getKey()).matches()
                                        && size.equals(
                                            new DataSize.Field().convert(equal.getValue()))))
                          return true;
                        if (smalls.entrySet().stream()
                            .anyMatch(
                                small ->
                                    small.getKey().matcher(e.getKey()).matches()
                                        && size.smallerThan(
                                            new DataSize.Field().convert(small.getValue()))))
                          return true;
                      }

                      if (e.getValue() instanceof LocalDateTime) {
                        var time = ((LocalDateTime) e.getValue());
                        if (larges.entrySet().stream()
                            .anyMatch(
                                large ->
                                    large.getKey().matcher(e.getKey()).matches()
                                        && time.compareTo(LocalDateTime.parse(large.getValue()))
                                            > 0)) return true;
                        if (equals.entrySet().stream()
                            .anyMatch(
                                equal ->
                                    equal.getKey().matcher(e.getKey()).matches()
                                        && time.compareTo(LocalDateTime.parse(equal.getValue()))
                                            == 0)) return true;
                        if (smalls.entrySet().stream()
                            .anyMatch(
                                small ->
                                    small.getKey().matcher(e.getKey()).matches()
                                        && time.compareTo(LocalDateTime.parse(small.getValue()))
                                            < 0)) return true;
                      }
                      return false;
                    }));
  }

  private static Optional<Predicate<Map<String, Object>>> forString(String text) {
    var patterns =
        Arrays.stream(text.split(","))
            // this is number comparison
            .filter(item -> !item.contains("=="))
            .flatMap(
                item -> {
                  var ss = item.trim().split("=");
                  if (ss.length == 2)
                    return Stream.of(
                        Map.entry(
                            Utils.wildcardToPattern(ss[0].trim()),
                            Utils.wildcardToPattern(ss[1].trim())));
                  return Stream.of();
                })
            .collect(Collectors.toList());
    if (patterns.isEmpty()) return Optional.empty();
    return Optional.of(
        item ->
            item.entrySet().stream()
                .anyMatch(
                    entry ->
                        patterns.stream()
                            .anyMatch(
                                ptns ->
                                    ptns.getKey().matcher(entry.getKey()).matches()
                                        && ptns.getValue()
                                            .matcher(entry.getValue().toString())
                                            .matches())));
  }
}
