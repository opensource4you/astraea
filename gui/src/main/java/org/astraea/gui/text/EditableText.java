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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import javafx.application.Platform;
import javafx.scene.Node;
import javafx.scene.control.TextInputControl;
import org.astraea.common.Utils;

public interface EditableText {

  static Builder multiline() {
    return new Builder(new javafx.scene.control.TextArea());
  }

  static Builder singleLine() {
    return new Builder(new javafx.scene.control.TextField());
  }

  class Builder {
    private final TextInputControl field;

    private boolean acceptEmpty = true;

    private Builder(TextInputControl field) {
      this.field = field;
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
      if (defaultValue != null) {
        field.setText(defaultValue);
        field.setFocusTraversable(false);
      }
      return this;
    }

    public Builder disable() {
      field.setDisable(true);
      return this;
    }

    public Builder hint(String hint) {
      field.setPromptText(hint);
      field.setFocusTraversable(false);
      return this;
    }

    public Builder disallowEmpty() {
      acceptEmpty = false;
      return this;
    }

    public EditableText build() {
      return new EditableText() {

        @Override
        public Node node() {
          return field;
        }

        @Override
        public void text(String text) {
          if (Platform.isFxApplicationThread()) field.setText(text);
          else Platform.runLater(() -> field.setText(text));
        }

        @Override
        public boolean valid() {
          return acceptEmpty || text().isPresent();
        }

        @Override
        public Optional<String> text() {
          return Optional.ofNullable(field.getText()).filter(s -> !s.isBlank());
        }

        @Override
        public void disable() {
          if (Platform.isFxApplicationThread()) field.setDisable(true);
          else Platform.runLater(() -> field.setDisable(true));
        }

        @Override
        public void enable() {
          if (Platform.isFxApplicationThread()) field.setDisable(false);
          else Platform.runLater(() -> field.setDisable(false));
        }
      };
    }
  }

  Node node();

  void text(String text);

  /** @return true if the current value is valid. Otherwise, return false */
  boolean valid();

  Optional<String> text();

  void disable();

  void enable();

  default void cleanup() {
    text("");
  }

  default void text(Throwable e) {
    if (e instanceof IllegalArgumentException) {
      // expected error so we just print message
      append(e.getMessage());
      return;
    }
    if (e instanceof CompletionException) {
      text(e.getCause());
      return;
    }
    if (e != null) text(Utils.toString(e));
  }

  default void append(String text) {
    if (text == null) return;
    text(
        text()
            .map(before -> "[" + formatCurrentTime() + "] " + text + "\n" + before)
            .orElseGet(() -> "[" + formatCurrentTime() + "] " + text));
  }

  private static String formatCurrentTime() {
    return LocalDateTime.ofInstant(
            Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.systemDefault())
        .toString();
  }
}
