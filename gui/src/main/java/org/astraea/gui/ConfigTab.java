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

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;

public class ConfigTab {

  private enum Resource {
    BROKER,
    TOPIC
  }

  public static Tab of(Context context) {
    var resources = Utils.radioButton(Resource.values());
    var pane =
        Utils.searchToTable(
            (word, console) -> {
              var isTopic = resources.get(Resource.TOPIC).isSelected();
              return context
                  .optionalAdmin()
                  .map(
                      admin ->
                          isTopic
                              ? admin.topics(admin.topicNames()).stream()
                                  .map(t -> Map.entry(t.name(), t.config()))
                              : admin.brokers().stream()
                                  .map(t -> Map.entry(String.valueOf(t.id()), t.config())))
                  .orElse(Stream.of())
                  .map(
                      e -> {
                        Map<String, Object> map = new LinkedHashMap<>();
                        map.put(isTopic ? "name" : "id", e.getKey());
                        e.getValue().raw().entrySet().stream()
                            .filter(entry -> word.isEmpty() || entry.getKey().contains(word))
                            .sorted(Map.Entry.comparingByKey())
                            .forEach(entry -> map.put(entry.getKey(), entry.getValue()));
                        return map;
                      })
                  .collect(Collectors.toList());
            },
            resources.values());
    var tab = new Tab("config");
    tab.setContent(pane);
    return tab;
  }
}
