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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Label;
import javafx.scene.control.RadioButton;
import javafx.scene.control.TextField;
import javafx.scene.control.ToggleGroup;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;

class Utils {

  public static TextField onlyNumber() {
    var field = new TextField();
    field
        .textProperty()
        .addListener(
            (observable, oldValue, newValue) -> {
              if (!newValue.matches("\\d*")) {
                field.setText(newValue.replaceAll("[^\\d]", ""));
              }
            });
    return field;
  }

  public static TextField copyableField(String content) {
    var field = new TextField(content);
    field.setEditable(false);
    return field;
  }

  public static Label label(String content) {
    return new Label(content);
  }

  public static HBox hbox(Node... nodes) {
    return hbox(Pos.CENTER_LEFT, nodes);
  }

  public static HBox hbox(Pos pos, Node... nodes) {
    var pane = new HBox(10);
    pane.setAlignment(pos);
    pane.getChildren().setAll(nodes);
    return pane;
  }

  public static VBox vbox(Node... nodes) {
    return vbox(Pos.TOP_LEFT, nodes);
  }

  public static VBox vbox(Pos pos, Node... nodes) {
    var pane = new VBox(10);
    pane.setPadding(new Insets(15));
    pane.getChildren().setAll(nodes);
    pane.setAlignment(pos);
    return pane;
  }

  public static String toString(Throwable e) {
    var sw = new StringWriter();
    var pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    return sw.toString();
  }

  public static String format(long timestamp) {
    if (timestamp > 0) {
      var format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
      return format.format(new Date(timestamp));
    }
    return "unknown";
  }

  public static <E extends Enum<E>, T extends Enum<E>> Map<T, RadioButton> radioButton(T[] keys) {
    var group = new ToggleGroup();
    var result = new HashMap<T, RadioButton>();
    for (var i = 0; i != keys.length; ++i) {
      var button = new RadioButton(keys[i].toString());
      button.setToggleGroup(group);
      button.setSelected(i == 0);
      result.put(keys[i], button);
    }
    return result;
  }

  private Utils() {}
}
