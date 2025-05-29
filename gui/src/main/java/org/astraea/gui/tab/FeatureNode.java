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
package org.astraea.gui.tab;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.geometry.Side;
import javafx.scene.Node;
import org.astraea.common.MapUtils;
import org.astraea.common.function.Bi3Function;
import org.astraea.gui.Context;
import org.astraea.gui.Logger;
import org.astraea.gui.pane.Argument;
import org.astraea.gui.pane.FirstPart;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.SecondPart;
import org.astraea.gui.pane.Slide;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.TextInput;

public class FeatureNode {
  private static final String FEATURE_NAME_KEY = "name";

  private static final String FINALIZED_VERSION_KEY = "finalized version";

  static Bi3Function<List<Map<String, Object>>, Argument, Logger, CompletionStage<Void>>
      tableViewAction(Context context) {
    return (items, inputs, logger) -> {
      var features =
          items.stream()
              .flatMap(
                  item -> {
                    var feature = item.get(FEATURE_NAME_KEY);
                    if (feature != null) return Stream.of((String) feature);
                    return Stream.of();
                  })
              .collect(Collectors.toSet());
      if (features.isEmpty()) {
        logger.log("nothing to alert");
        return CompletableFuture.completedStage(null);
      }
      var finalizedVersion = inputs.get(FINALIZED_VERSION_KEY).map(Short::parseShort);
      if (finalizedVersion.isEmpty()) {
        logger.log("please define " + FINALIZED_VERSION_KEY);
        return CompletableFuture.completedStage(null);
      }

      return context
          .admin()
          .feature(
              features.stream()
                  .collect(Collectors.toMap(Function.identity(), ignored -> finalizedVersion.get())))
          .thenApply(
              v -> {
                logger.log("succeed to alter finalized version: " + finalizedVersion.get());
                return null;
              });
    };
  }

  private static Node basicNode(Context context) {
    var firstPart =
        FirstPart.builder()
            .clickName("REFRESH")
            .tableRefresher(
                (argument, logger) ->
                    context
                        .admin()
                        .feature()
                        .thenApply(
                            featureInfo -> {
                              var result = new ArrayList<Map<String, Object>>();
                              featureInfo
                                  .finalizedFeatures()
                                  .forEach(
                                      (key, value) ->
                                          result.add(
                                              MapUtils.of(
                                                  FEATURE_NAME_KEY,
                                                  key,
                                                  "status",
                                                  "finalized",
                                                  "min",
                                                  value.min(),
                                                  "max",
                                                  value.max())));
                              featureInfo
                                  .supportedFeatures()
                                  .forEach(
                                      (key, value) ->
                                          result.add(
                                              MapUtils.of(
                                                  FEATURE_NAME_KEY,
                                                  key,
                                                  "status",
                                                  "supported",
                                                  "min",
                                                  value.min(),
                                                  "max",
                                                  value.max())));
                              return result;
                            }))
            .build();
    var secondPart =
        SecondPart.builder()
            .textInputs(
                List.of(TextInput.of(FINALIZED_VERSION_KEY, EditableText.singleLine().disable().build())))
            .buttonName("ALTER")
            .action(tableViewAction(context))
            .build();
    return PaneBuilder.of().firstPart(firstPart).secondPart(secondPart).build();
  }

  public static Node of(Context context) {
    return Slide.of(Side.TOP, MapUtils.of("basic", basicNode(context))).node();
  }
}
