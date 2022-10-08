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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.collections.FXCollections;
import javafx.event.EventHandler;
import javafx.geometry.HPos;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.Labeled;
import javafx.scene.control.RadioButton;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.ToggleButton;
import javafx.scene.control.ToggleGroup;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.scene.input.KeyEvent;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Pane;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import org.astraea.common.LinkedHashMap;

/** a template layout for all tabs. */
public class PaneBuilder {

  public static PaneBuilder of() {
    return new PaneBuilder();
  }

  private static final int MAX_NUMBER_OF_TEXT_FIELD_ONE_LINE = 3;

  private List<RadioButton> radioButtons = new ArrayList<>();

  private final Set<String> textKeys = new LinkedHashSet<>();
  private final Map<String, Boolean> textPriority = new LinkedHashMap<>();
  private final Map<String, Boolean> textNumberOnly = new LinkedHashMap<>();
  private Label searchLabel = null;
  private final TextField searchField = new TextField();

  private Button actionButton = new Button("SEARCH");

  private TableView<Map<String, Object>> tableView = null;

  private final ConsoleArea console = new ConsoleArea();

  private Function<Input, Object> actionRunner = null;

  private PaneBuilder() {}

  public PaneBuilder radioButtons(Set<String> keys) {
    if (keys.isEmpty()) return this;
    var group = new ToggleGroup();
    radioButtons =
        keys.stream()
            .map(
                key -> {
                  var button = new RadioButton(key);
                  button.setToggleGroup(group);
                  return button;
                })
            .collect(Collectors.toList());
    radioButtons.get(0).setSelected(true);
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
    textKeys.add(key);
    textPriority.put(key, required);
    textNumberOnly.put(key, numberOnly);
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

  public PaneBuilder searchField(String hint) {
    searchLabel = new Label(hint);
    return this;
  }

  public PaneBuilder buttonName(String name) {
    actionButton = new Button(name);
    return this;
  }

  public PaneBuilder outputMessage(Function<Input, CompletionStage<String>> outputMessage) {
    this.actionRunner = input -> (OutputMessage) () -> outputMessage.apply(input);
    return this;
  }

  public PaneBuilder outputTable(
      Function<Input, CompletionStage<List<Map<String, Object>>>> outputTable) {
    this.actionRunner = input -> (OutputTable) () -> outputTable.apply(input);
    return enableTableView();
  }

  public PaneBuilder output(Function<Input, Output> output) {
    this.actionRunner = input -> (Object) output.apply(input);
    return enableTableView();
  }

  private PaneBuilder enableTableView() {
    tableView = new TableView<>(FXCollections.observableArrayList());
    tableView.getSelectionModel().setCellSelectionEnabled(true);
    tableView.setOnKeyPressed(new CopyCellToClipboard());
    return this;
  }

  public Pane build() {
    var nodes = new ArrayList<Node>();
    if (!radioButtons.isEmpty())
      nodes.add(Utils.hbox(Pos.CENTER, radioButtons.toArray(Node[]::new)));
    var textFields = new LinkedHashMap<String, Supplier<String>>();
    if (!textKeys.isEmpty()) {
      var paneAndFields = pane(textKeys, textPriority, textNumberOnly);
      nodes.add(paneAndFields.getKey());
      textFields.putAll(paneAndFields.getValue());
    }
    if (searchLabel != null)
      nodes.add(Utils.hbox(Pos.CENTER, searchLabel, searchField, actionButton));
    else nodes.add(actionButton);
    if (tableView != null) nodes.add(tableView);
    nodes.add(console);

    Runnable handler =
        actionRunner == null
            ? null
            : () -> {
              var selectedRadio =
                  radioButtons.stream()
                      .filter(ToggleButton::isSelected)
                      .map(Labeled::getText)
                      .findFirst();
              var inputTexts =
                  textFields.entrySet().stream()
                      .flatMap(
                          entry ->
                              Optional.ofNullable(entry.getValue().get())
                                  .filter(v -> !v.isBlank())
                                  .map(v -> Map.entry(entry.getKey(), v))
                                  .stream())
                      .collect(
                          Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
              var requiredNonexistentKeys =
                  textPriority.entrySet().stream()
                      .filter(entry -> entry.getValue() && !inputTexts.containsKey(entry.getKey()))
                      .map(Map.Entry::getKey)
                      .collect(Collectors.toSet());
              if (!requiredNonexistentKeys.isEmpty()) {
                console.text("Please define required fields: " + requiredNonexistentKeys);
                return;
              }

              var searchPattern =
                  searchLabel == null
                      ? Optional.<Pattern>empty()
                      : Optional.ofNullable(searchField.getText())
                          .filter(s -> !s.isBlank())
                          .map(PaneBuilder::wildcardToPattern);
              var input =
                  new Input() {
                    @Override
                    public Optional<String> selectedRadio() {
                      return selectedRadio;
                    }

                    @Override
                    public Map<String, String> texts() {
                      return inputTexts;
                    }

                    @Override
                    public boolean matchSearch(String word) {
                      return searchPattern.map(p -> p.matcher(word).matches()).orElse(true);
                    }

                    @Override
                    public void log(String message) {
                      console.append(message);
                    }
                  };
              BiConsumer<OutputTable, Boolean> processOutputTable =
                  (outputTable, enableButton) ->
                      outputTable
                          .table()
                          .whenComplete(
                              (table, exception) -> {
                                if (exception != null) {
                                  console.text(exception);
                                  Platform.runLater(() -> actionButton.setDisable(false));
                                  return;
                                }
                                var columns =
                                    table.stream()
                                        .flatMap(r -> r.keySet().stream())
                                        .collect(Collectors.toCollection(LinkedHashSet::new))
                                        .stream()
                                        .map(
                                            key -> {
                                              var col =
                                                  new TableColumn<Map<String, Object>, Object>(key);
                                              col.setCellValueFactory(
                                                  param ->
                                                      new ReadOnlyObjectWrapper<>(
                                                          param.getValue().getOrDefault(key, "")));
                                              return col;
                                            })
                                        .collect(Collectors.toList());
                                Platform.runLater(
                                    () -> {
                                      tableView.getColumns().setAll(columns);
                                      tableView.getItems().setAll(table);
                                      actionButton.setDisable(!enableButton);
                                    });
                              });

              BiConsumer<OutputMessage, Boolean> processOutputMessage =
                  (outputMessage, enableButton) ->
                      outputMessage
                          .message()
                          .whenComplete(
                              (message, exception) -> {
                                if (exception != null) {
                                  console.text(exception);
                                  Platform.runLater(() -> actionButton.setDisable(false));
                                  return;
                                }
                                console.append(message);
                                if (enableButton)
                                  Platform.runLater(() -> actionButton.setDisable(false));
                              });

              console.cleanup();
              actionButton.setDisable(true);
              try {
                Object outputObject = actionRunner.apply(input);
                if (outputObject instanceof Output) {
                  processOutputTable.accept((OutputTable) outputObject, false);
                  processOutputMessage.accept((OutputMessage) outputObject, true);
                  return;
                }

                if (outputObject instanceof OutputTable)
                  processOutputTable.accept((OutputTable) outputObject, true);

                if (outputObject instanceof OutputMessage)
                  processOutputMessage.accept((OutputMessage) outputObject, true);
              } catch (IllegalArgumentException e) {
                console.append(e.getMessage());
                actionButton.setDisable(false);
              } catch (Exception e) {
                console.append(e);
                actionButton.setDisable(false);
              }
            };

    if (handler != null) {
      actionButton.setOnAction(ignored -> handler.run());
      // there is only one text field, so we register the ENTER event.
      if (textFields.isEmpty())
        searchField.setOnKeyPressed(
            event -> {
              if (event.getCode().equals(KeyCode.ENTER)) handler.run();
            });
    }

    return Utils.vbox(Pos.CENTER, nodes.toArray(Node[]::new));
  }

  private static Map.Entry<Pane, LinkedHashMap<String, Supplier<String>>> pane(
      Set<String> textKeys,
      Map<String, Boolean> textPriority,
      Map<String, Boolean> textNumberOnly) {
    Function<String, Label> labelFunction =
        (key) -> {
          if (textPriority.get(key)) {
            var label = new Label(key + "*");
            label.setFont(Font.font("Verdana", FontWeight.EXTRA_BOLD, 12));
            return label;
          }
          return new Label(key);
        };

    Function<String, TextField> textFunction =
        (key) -> textNumberOnly.get(key) ? Utils.onlyNumber() : new TextField();

    var textFields = new LinkedHashMap<String, Supplier<String>>();
    if (textKeys.size() <= 3) {
      var pane = new GridPane();
      pane.setAlignment(Pos.CENTER);
      var row = 0;
      for (var key : textKeys) {
        var label = labelFunction.apply(key);
        var textField = textFunction.apply(key);
        textFields.put(key, textField::getText);
        GridPane.setHalignment(label, HPos.RIGHT);
        GridPane.setMargin(label, new Insets(10, 5, 10, 15));
        pane.add(label, 0, row);

        GridPane.setHalignment(textField, HPos.LEFT);
        GridPane.setMargin(textField, new Insets(10, 15, 10, 5));
        pane.add(textField, 1, row);
        row++;
      }
      return Map.entry(pane, textFields);
    }

    var pane = new GridPane();
    pane.setAlignment(Pos.CENTER);
    var row = 0;
    var column = 0;
    var count = 0;
    for (var key : textKeys) {
      if (count >= MAX_NUMBER_OF_TEXT_FIELD_ONE_LINE) {
        count = 0;
        column = 0;
        row++;
      }

      var label = labelFunction.apply(key);
      var textField = textFunction.apply(key);
      textFields.put(key, textField::getText);
      GridPane.setHalignment(label, HPos.RIGHT);
      GridPane.setMargin(label, new Insets(10, 5, 10, 15));
      pane.add(label, column++, row);

      GridPane.setHalignment(textField, HPos.LEFT);
      GridPane.setMargin(textField, new Insets(10, 15, 10, 5));
      pane.add(textField, column++, row);
      count++;
    }
    return Map.entry(pane, textFields);
  }

  static Pattern wildcardToPattern(String string) {
    return Pattern.compile(string.replaceAll("\\?", ".").replaceAll("\\*", ".*"));
  }

  interface Input {
    Optional<String> selectedRadio();

    Map<String, String> texts();

    boolean matchSearch(String word);

    void log(String message);
  }

  /** use this interface if the tab show everything on the table */
  interface OutputMessage {
    CompletionStage<String> message();
  }

  /** use this interface if the tab does not need show table data */
  interface OutputTable {
    CompletionStage<List<Map<String, Object>>> table();
  }

  /**
   * use this interface if the tab needs to refresh table first and then output result to console.
   */
  interface Output extends OutputMessage, OutputTable {}

  private static class CopyCellToClipboard implements EventHandler<KeyEvent> {

    private final KeyCodeCombination keyForWindows =
        new KeyCodeCombination(KeyCode.C, KeyCombination.CONTROL_DOWN);

    private final KeyCodeCombination keyForMacos =
        new KeyCodeCombination(KeyCode.C, KeyCombination.SHORTCUT_DOWN);

    @SuppressWarnings("unchecked")
    public void handle(final KeyEvent keyEvent) {
      if (keyForWindows.match(keyEvent) || keyForMacos.match(keyEvent)) {
        if (keyEvent.getSource() instanceof TableView) {
          var tableView = (TableView) keyEvent.getSource();
          var pos = tableView.getFocusModel().getFocusedCell();
          var value =
              pos.getTableColumn()
                  .getCellObservableValue(tableView.getItems().get(pos.getRow()))
                  .getValue();
          var clipboardContent = new ClipboardContent();
          clipboardContent.putString(value.toString());
          Clipboard.getSystemClipboard().setContent(clipboardContent);
        }
      }
    }
  }
}
