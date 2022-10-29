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
package org.astraea.gui.button;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javafx.scene.Node;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ToggleGroup;
import org.astraea.gui.pane.Lattice;

public interface SelectBox {

  static SelectBox single(List<String> keys, int sizeOfColumns) {
    var group = new ToggleGroup();
    var selectedKeys = Collections.synchronizedCollection(new ArrayList<String>());
    var items =
        keys.stream()
            .map(
                key -> {
                  var box = new javafx.scene.control.RadioButton(key);
                  box.setToggleGroup(group);
                  box.selectedProperty()
                      .addListener(
                          (o, previous, now) -> {
                            if (box.isSelected()) selectedKeys.add(box.getText());
                            else selectedKeys.remove(box.getText());
                          });
                  return box;
                })
            .collect(Collectors.toUnmodifiableList());
    items.get(0).setSelected(true);
    var node =
        Lattice.of(items.stream().map(m -> (Node) m).collect(Collectors.toList()), sizeOfColumns)
            .node();
    return of(() -> selectedKeys, node);
  }

  static SelectBox multi(List<String> keys, int sizeOfColumns) {
    var selectedKeys = Collections.synchronizedCollection(new ArrayList<String>());
    var items =
        keys.stream()
            .map(
                key -> {
                  var box = new CheckBox(key);
                  box.selectedProperty()
                      .addListener(
                          (o, previous, now) -> {
                            if (box.isSelected()) selectedKeys.add(box.getText());
                            else selectedKeys.remove(box.getText());
                          });
                  return box;
                })
            .collect(Collectors.toUnmodifiableList());
    var node =
        Lattice.of(items.stream().map(m -> (Node) m).collect(Collectors.toList()), sizeOfColumns)
            .node();
    return of(() -> selectedKeys, node);
  }

  private static SelectBox of(Supplier<Collection<String>> selectedKeysSupplier, Node node) {
    return new SelectBox() {
      @Override
      public List<String> selectedKeys() {
        return List.copyOf(selectedKeysSupplier.get());
      }

      @Override
      public Node node() {
        return node;
      }
    };
  }

  List<String> selectedKeys();

  Node node();
}
