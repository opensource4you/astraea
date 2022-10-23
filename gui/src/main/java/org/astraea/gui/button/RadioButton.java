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

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javafx.scene.control.ToggleGroup;

public class RadioButton extends javafx.scene.control.RadioButton {

  public static List<RadioButton> single(List<Object> objs) {
    if (objs.isEmpty())
      throw new IllegalArgumentException("can't build radio button with no objects");
    var group = new ToggleGroup();
    var buttons =
        objs.stream()
            .map(
                obj -> {
                  var button = new RadioButton(obj);
                  button.setToggleGroup(group);
                  return button;
                })
            .collect(Collectors.toList());

    buttons.get(0).setSelected(true);
    return buttons;
  }

  public static List<RadioButton> multi(List<Object> objs) {
    if (objs.isEmpty())
      throw new IllegalArgumentException("can't build radio button with no objects");
    var buttons = objs.stream().map(RadioButton::new).collect(Collectors.toList());
    buttons.get(0).setSelected(true);
    return buttons;
  }

  private final Object obj;

  private RadioButton(Object obj) {
    super(obj.toString());
    this.obj = obj;
  }

  public Optional<Object> selectedObject() {
    if (this.isSelected()) return Optional.ofNullable(obj);
    return Optional.empty();
  }
}
