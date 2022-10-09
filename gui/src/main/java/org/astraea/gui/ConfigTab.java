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

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.LinkedHashSet;

public class ConfigTab {

  private enum Resource {
    BROKER("broker"),
    TOPIC("topic");

    private final String alias;

    Resource(String alias) {
      this.alias = alias;
    }
  }

  public static Tab of(Context context) {
    var pane =
        PaneBuilder.of()
            .radioButtons(
                LinkedHashSet.of(
                    Arrays.stream(Resource.values()).map(r -> r.alias).toArray(String[]::new)))
            .searchField("config key")
            .buttonTableAction(
                input -> {
                  var isTopic =
                      input.selectedRadio().map(Resource.TOPIC.alias::equals).orElse(true);
                  return context
                      .submit(
                          admin ->
                              isTopic
                                  ? admin
                                      .topicNames(true)
                                      .thenCompose(admin::topics)
                                      .thenApply(
                                          topics ->
                                              topics.stream()
                                                  .map(t -> Map.entry(t.name(), t.config())))
                                  : admin
                                      .brokers()
                                      .thenApply(
                                          brokers ->
                                              brokers.stream()
                                                  .map(
                                                      t ->
                                                          Map.entry(
                                                              String.valueOf(t.id()), t.config()))))
                      .thenApply(
                          items ->
                              items
                                  .map(
                                      e -> {
                                        var map = new LinkedHashMap<String, Object>();
                                        map.put(isTopic ? "name" : "broker id", e.getKey());
                                        e.getValue().raw().entrySet().stream()
                                            .filter(entry -> input.matchSearch(entry.getKey()))
                                            .sorted(Map.Entry.comparingByKey())
                                            .forEach(
                                                entry -> map.put(entry.getKey(), entry.getValue()));
                                        return map;
                                      })
                                  .collect(Collectors.toList()));
                })
            .build();
    var tab = new Tab("config");
    tab.setContent(pane);
    return tab;
  }
}
