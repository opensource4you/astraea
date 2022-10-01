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
import org.astraea.common.admin.Admin;

public class BrokerConfigTab {

  public static Tab of(Context context) {
    var pane =
        Utils.searchToTable(
            "search for config:",
            word -> {
              var brokers = context.optionalAdmin().map(Admin::brokers).orElse(List.of());
              return brokers.stream()
                  .map(
                      n -> {
                        Map<String, String> map = new LinkedHashMap<>();
                        map.put("id", String.valueOf(n.id()));
                        n.config().raw().entrySet().stream()
                            .filter(entry -> word.isEmpty() || entry.getKey().contains(word))
                            .sorted(Map.Entry.comparingByKey())
                            .forEach(entry -> map.put(entry.getKey(), entry.getValue()));
                        return map;
                      })
                  .collect(Collectors.toList());
            });
    var tab = new Tab("broker config");
    tab.setContent(pane);
    return tab;
  }
}
