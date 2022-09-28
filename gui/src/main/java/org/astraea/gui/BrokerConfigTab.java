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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.scene.control.Tab;

public class BrokerConfigTab {

  public static Tab of(Context context) {
    var pane =
        context.tableView(
            "search for config:",
            (admin, word) -> {
              var beans =
                  admin.nodes().stream()
                      .map(
                          n ->
                              new Bean(
                                  String.valueOf(n.id()),
                                  n.config().raw().entrySet().stream()
                                      .filter(
                                          entry -> word.isEmpty() || entry.getKey().contains(word))
                                      .collect(
                                          Collectors.toMap(
                                              Map.Entry::getKey, Map.Entry::getValue))))
                      .collect(Collectors.toList());
              var columnGetter = new LinkedHashMap<String, Function<Bean, Object>>();
              columnGetter.put("broker id", bean -> bean.name);
              beans.stream()
                  .flatMap(bean -> bean.configs.keySet().stream())
                  .sorted()
                  .forEach(
                      key -> columnGetter.put(key, bean -> bean.configs.getOrDefault(key, "")));
              return Context.result(columnGetter, beans);
            });
    var tab = new Tab("broker config");
    tab.setContent(pane);
    return tab;
  }

  public static class Bean {
    private final String name;
    private final Map<String, String> configs;

    public Bean(String name, Map<String, String> configs) {
      this.name = name;
      this.configs = configs;
    }
  }
}
