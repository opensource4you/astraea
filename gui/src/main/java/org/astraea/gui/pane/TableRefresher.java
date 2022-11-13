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
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import org.astraea.gui.Logger;

@FunctionalInterface
public interface TableRefresher {

  String BASIC_KEY = "basic";

  static TableRefresher of(
      BiFunction<Argument, Logger, CompletionStage<List<Map<String, Object>>>> refresher) {
    return (argument, logger) ->
        refresher
            .apply(argument, logger)
            .thenApply(r -> r.isEmpty() ? Map.of() : Map.of(BASIC_KEY, r));
  }

  CompletionStage<Map<String, List<Map<String, Object>>>> apply(Argument argument, Logger logger);
}
