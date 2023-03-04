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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.astraea.common.function.Bi3Function;
import org.astraea.gui.Logger;
import org.astraea.gui.text.TextInput;

public class SecondPart {

  public static Builder builder() {
    return new Builder();
  }

  private final List<TextInput> textInputs;

  private final String buttonName;
  private final Bi3Function<List<Map<String, Object>>, Argument, Logger, CompletionStage<Void>>
      action;

  private SecondPart(
      List<TextInput> textInputs,
      String buttonName,
      Bi3Function<List<Map<String, Object>>, Argument, Logger, CompletionStage<Void>> action) {
    this.textInputs = textInputs;
    this.buttonName = buttonName;
    this.action = action;
  }

  public List<TextInput> textInputs() {
    return textInputs;
  }

  public String buttonName() {
    return buttonName;
  }

  public Bi3Function<List<Map<String, Object>>, Argument, Logger, CompletionStage<Void>> action() {
    return action;
  }

  public static class Builder {
    private List<TextInput> textInputs = List.of();

    private String buttonName = "EXECUTE";
    private Bi3Function<List<Map<String, Object>>, Argument, Logger, CompletionStage<Void>> action =
        (x, y, z) -> CompletableFuture.completedFuture(null);

    private Builder() {}

    public Builder textInputs(List<TextInput> textInputs) {
      this.textInputs = textInputs;
      return this;
    }

    public Builder buttonName(String buttonName) {
      this.buttonName = buttonName;
      return this;
    }

    public Builder action(
        Bi3Function<List<Map<String, Object>>, Argument, Logger, CompletionStage<Void>> action) {
      this.action = action;
      return this;
    }

    public SecondPart build() {
      return new SecondPart(textInputs, buttonName, action);
    }
  }
}
