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
package org.astraea.gui.text;

import javafx.application.Platform;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;

public class Label extends javafx.scene.control.Label implements Comparable<Label> {

  public static Label of(String content) {
    return new Label(false, content, content);
  }

  public static Label highlight(String content) {
    var label = new Label(true, content, content + "*");
    label.setFont(Font.font("Verdana", FontWeight.EXTRA_BOLD, 12));
    return label;
  }

  private final boolean highlight;
  private final String key;

  private Label(boolean highlight, String key, String content) {
    super(content);
    this.key = key;
    this.highlight = highlight;
  }

  /** @return the key associated to this label. Noted that the key may be different from text */
  public String key() {
    return key;
  }

  public boolean highlight() {
    return highlight;
  }

  @Override
  public int compareTo(Label o) {
    return key.compareTo(o.key);
  }

  public void text(String text) {
    if (Platform.isFxApplicationThread()) setText(text);
    else Platform.runLater(() -> setText(text));
  }
}
