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

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.scene.control.ComboBox;

public class IntegerBox extends ComboBox<Integer> {

  public IntegerBox() {
    super(FXCollections.observableArrayList());
  }

  public IntegerBox(int initialValue) {
    super(FXCollections.observableArrayList(initialValue));
  }

  void range(int from, int to) {
    values(IntStream.range(from, to).mapToObj(i -> i + 1).collect(Collectors.toList()));
  }

  void values(Collection<Integer> values) {
    if (Platform.isFxApplicationThread()) getItems().setAll(values);
    else Platform.runLater(() -> getItems().setAll(values));
  }
}
