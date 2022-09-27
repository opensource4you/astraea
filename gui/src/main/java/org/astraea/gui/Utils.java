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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import javafx.scene.control.TextField;

class Utils {

  public static String toString(Throwable e) {
    var sw = new StringWriter();
    var pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    return sw.toString();
  }

  public static TextField onlyNumber(int defaultValue) {
    var field = new TextField(String.valueOf(defaultValue));
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

  public static <T> List<T> random(List<T> all, int number) {
    var sub = all.size() - number;
    if (sub < 0)
      throw new IllegalArgumentException(
          "only " + all.size() + " elements, but required number is " + number);
    if (sub == 0) return all;
    var result = new LinkedList<>(all);
    Collections.shuffle(result);
    return result.stream().skip(sub).collect(Collectors.toUnmodifiableList());
  }

  private Utils() {}
}
