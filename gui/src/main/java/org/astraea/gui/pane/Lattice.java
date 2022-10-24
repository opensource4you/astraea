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

import java.util.Map;
import javafx.geometry.HPos;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.layout.GridPane;
import org.astraea.gui.text.KeyLabel;
import org.astraea.gui.text.TextInput;

public interface Lattice {

  static Lattice of(Map<KeyLabel, TextInput> nodePair, int maxPairOneLine) {
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
      GridPane.setHalignment(pair.getKey().node(), HPos.RIGHT);
      GridPane.setMargin(pair.getKey().node(), new Insets(10, 5, 10, 15));
      pane.add(pair.getKey().node(), column++, row);
      GridPane.setHalignment(pair.getValue().node(), HPos.LEFT);
      GridPane.setMargin(pair.getValue().node(), new Insets(10, 15, 10, 5));
      pane.add(pair.getValue().node(), column++, row);
      count++;
    }
    return new Lattice() {

      @Override
      public Map<KeyLabel, TextInput> content() {
        return Map.copyOf(nodePair);
      }

      @Override
      public Node node() {
        return pane;
      }
    };
  }

  static Lattice singleColumn(Map<KeyLabel, TextInput> nodePair) {
    var pane = new GridPane();
    pane.setAlignment(Pos.CENTER);
    var row = 0;
    for (var pair : nodePair.entrySet()) {
      GridPane.setHalignment(pair.getKey().node(), HPos.RIGHT);
      GridPane.setMargin(pair.getKey().node(), new Insets(10, 5, 10, 15));
      pane.add(pair.getKey().node(), 0, row);

      GridPane.setHalignment(pair.getValue().node(), HPos.LEFT);
      GridPane.setMargin(pair.getValue().node(), new Insets(10, 15, 10, 5));
      pane.add(pair.getValue().node(), 1, row);
      row++;
    }
    return new Lattice() {

      @Override
      public Map<KeyLabel, TextInput> content() {
        return Map.copyOf(nodePair);
      }

      @Override
      public Node node() {
        return pane;
      }
    };
  }

  Map<KeyLabel, TextInput> content();

  Node node();
}
