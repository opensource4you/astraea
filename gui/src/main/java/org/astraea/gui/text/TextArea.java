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
import java.util.concurrent.CompletionException;
import javafx.application.Platform;
import org.astraea.common.Utils;

public class TextArea extends javafx.scene.control.TextArea {

  public static TextArea of() {
    return new TextArea();
  }

  private TextArea() {
    super();
  }

  public void text(Throwable e) {
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

  public void text(String text) {
    text(text, false);
  }

  private void text(String text, boolean append) {
    if (text == null) return;
    Runnable exec =
        () -> {
          var before = getText();
          if (before != null && !before.isEmpty() && append)
            setText("[" + formatCurrentTime() + "] " + text + "\n" + before);
          else setText("[" + formatCurrentTime() + "] " + text);
        };
    if (Platform.isFxApplicationThread()) exec.run();
    else Platform.runLater(exec);
  }

  public void append(String text) {
    text(text, true);
  }

  public void cleanup() {
    if (Platform.isFxApplicationThread()) setText("");
    else Platform.runLater(() -> setText(""));
  }

  private static String formatCurrentTime() {
    return LocalDateTime.ofInstant(
            Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.systemDefault())
        .toString();
  }
}
