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
import java.util.Map;
import javafx.geometry.Side;
import javafx.scene.Node;
import org.astraea.common.MapUtils;
import org.astraea.gui.Context;
import org.astraea.gui.pane.FirstPart;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Slide;

public class FeatureNode {

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
                                                  "name",
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
                                                  "name",
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
    return PaneBuilder.of().firstPart(firstPart).build();
  }

  public static Node of(Context context) {
    return Slide.of(Side.TOP, MapUtils.of("basic", basicNode(context))).node();
  }
}
