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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;

public class TopicConfigTab {

  public static Tab of(Context context) {
    var pane =
        Utils.searchToTable(
            "config key (space means all configs):",
            (word, console) -> {
              var topics =
                  context
                      .optionalAdmin()
                      .map(admin -> admin.topics(admin.topicNames()))
                      .orElse(List.of());
              return topics.stream()
                  .map(
                      t -> {
                        Map<String, String> map = new LinkedHashMap<>();
                        map.put("name", t.name());
                        t.config().raw().entrySet().stream()
                            .filter(entry -> word.isEmpty() || entry.getKey().contains(word))
                            .sorted(Map.Entry.comparingByKey())
                            .forEach(entry -> map.put(entry.getKey(), entry.getValue()));
                        return map;
                      })
                  .collect(Collectors.toList());
            });
    var tab = new Tab("topic config");
    tab.setContent(pane);
    return tab;
  }
}
