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
import java.util.stream.Collectors;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.Admin;

public class SettingTab {

  public static Tab of(Context context) {
    var tab = new Tab("bootstrap servers");

    tab.setContent(
        Utils.searchToTable(
            (bootstrapServers, console) -> {
              if (bootstrapServers.isEmpty()) return List.of();
              var newAdmin = Admin.of(bootstrapServers);
              var brokerIds = newAdmin.brokerIds();
              var previous = context.replace(newAdmin);
              previous.ifPresent(admin -> org.astraea.common.Utils.swallowException(admin::close));
              return newAdmin.nodes().stream()
                  .map(
                      n ->
                          LinkedHashMap.<String, Object>of(
                              "id", n.id(), "host", n.host(), "port", n.port()))
                  .collect(Collectors.toUnmodifiableList());
            }));
    return tab;
  }
}
