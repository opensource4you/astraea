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
import java.util.stream.IntStream;
import javafx.geometry.HPos;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Labeled;
import javafx.scene.control.TextInputControl;
import org.astraea.common.MapUtils;

public class GridPane extends javafx.scene.layout.GridPane {

  public static GridPane of(Map<? extends Node, ? extends Node> nodePair, int maxPairOneLine) {
    var pane = new GridPane();
    pane.setAlignment(Pos.CENTER);
    var row = 0;
    var column = 0;
    var count = 0;
    for (var pair : nodePair.entrySet()) {
      if (count >= maxPairOneLine) {
        count = 0;
        column = 0;
        row++;
      }
      GridPane.setHalignment(pair.getKey(), HPos.RIGHT);
      GridPane.setMargin(pair.getKey(), new Insets(10, 5, 10, 15));
      pane.add(pair.getKey(), column++, row);
      GridPane.setHalignment(pair.getValue(), HPos.LEFT);
      GridPane.setMargin(pair.getValue(), new Insets(10, 15, 10, 5));
      pane.add(pair.getValue(), column++, row);
      count++;
    }
    return pane;
  }

  public static GridPane singleColumn(Map<? extends Node, ? extends Node> nodePair) {
    var pane = new GridPane();
    pane.setAlignment(Pos.CENTER);
    var row = 0;
    for (var pair : nodePair.entrySet()) {
      GridPane.setHalignment(pair.getKey(), HPos.RIGHT);
      GridPane.setMargin(pair.getKey(), new Insets(10, 5, 10, 15));
      pane.add(pair.getKey(), 0, row);

      GridPane.setHalignment(pair.getValue(), HPos.LEFT);
      GridPane.setMargin(pair.getValue(), new Insets(10, 15, 10, 5));
      pane.add(pair.getValue(), 1, row);
      row++;
    }
    return pane;
  }

  private GridPane() {}

  public Map<String, String> contents() {
    var elements = List.copyOf(getChildren());
    if (elements.isEmpty()) return Map.of();
    if (elements.size() % 2 != 0)
      throw new IllegalArgumentException(
          "the number of elements in GridPane must be 2n, but current is " + elements.size());
    return IntStream.range(0, elements.size() / 2)
        .mapToObj(
            index -> {
              var key = tryText(elements.get(2 * index));
              var value = tryText(elements.get(2 * index + 1));
              if (key.isPresent() && value.isPresent())
                return Optional.of(Map.entry(key.get(), value.get()));
              return Optional.<Map.Entry<String, String>>empty();
            })
        .flatMap(Optional::stream)
        .collect(MapUtils.toLinkedHashMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static Optional<String> tryText(Node node) {
    if (node instanceof TextInputControl)
      return Optional.ofNullable(((TextInputControl) node).getText());
    if (node instanceof Labeled) return Optional.ofNullable(((Labeled) node).getText());
    return Optional.empty();
  }
}
