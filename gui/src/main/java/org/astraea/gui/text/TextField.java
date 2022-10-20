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

import java.util.Optional;
import javafx.application.Platform;

public class TextField extends javafx.scene.control.TextField {

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final TextField field = new TextField();

    private Builder() {}

    public Builder placeholder(String placeholder) {
      field
          .textProperty()
          .addListener(
              (observable, oldValue, newValue) -> {
                if (newValue == null || newValue.isBlank()) {
                  field.text(placeholder);
                }
              });
      field.text(placeholder);
      return this;
    }

    public Builder onlyNumber() {
      field
          .textProperty()
          .addListener(
              (observable, oldValue, newValue) -> {
                if (newValue != null && !newValue.matches("\\d*")) {
                  field.setText(newValue.replaceAll("\\D", ""));
                }
              });
      return this;
    }

    public Builder defaultValue(String defaultValue) {
      field.text(defaultValue);
      return this;
    }

    public Builder disable() {
      field.disable();
      return this;
    }

    public TextField build() {
      return field;
    }
  }

  public static TextField of() {
    return new TextField();
  }

  public static TextField of(String content) {
    return new TextField(content);
  }

  private TextField() {
    super();
  }

  private TextField(String content) {
    super(content);
  }

  public void text(String text) {
    if (Platform.isFxApplicationThread()) setText(text);
    else Platform.runLater(() -> setText(text));
  }

  public Optional<String> text() {
    return Optional.ofNullable(getText()).filter(r -> !r.isBlank());
  }

  public void disable() {
    if (Platform.isFxApplicationThread()) setDisable(true);
    else Platform.runLater(() -> setDisable(true));
  }

  public void enable() {
    if (Platform.isFxApplicationThread()) setDisable(false);
    else Platform.runLater(() -> setDisable(false));
  }
}
