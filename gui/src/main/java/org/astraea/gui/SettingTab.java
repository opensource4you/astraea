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

import java.util.Optional;
import javafx.scene.control.Tab;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.metrics.MBeanClient;

public class SettingTab {

  private static final String BOOTSTRAP_SERVERS = "bootstrap servers";
  private static final String JMX_PORT = "jmx port";

  public static Tab of(Context context) {
    var pane =
        PaneBuilder.of()
            .input(BOOTSTRAP_SERVERS, true, false)
            .input(JMX_PORT, false, true)
            .buttonName("CHECK")
            .buttonMessageAction(
                input -> {
                  var bootstrapServers = input.texts().get(BOOTSTRAP_SERVERS);
                  var jmxPort =
                      Optional.ofNullable(input.texts().get(JMX_PORT)).map(Integer::parseInt);
                  var newAdmin = AsyncAdmin.of(bootstrapServers);
                  return newAdmin
                      .nodeInfos()
                      .thenApply(
                          nodeInfos -> {
                            context
                                .replace(newAdmin)
                                .ifPresent(
                                    admin ->
                                        org.astraea.common.Utils.swallowException(admin::close));
                            if (jmxPort.isEmpty())
                              return "succeed to connect to " + bootstrapServers;
                            nodeInfos.forEach(
                                n -> {
                                  try (var client = MBeanClient.jndi(n.host(), jmxPort.get())) {
                                    client.listDomains();
                                  }
                                });
                            context.replace(jmxPort.get());
                            return "succeed to connect to "
                                + bootstrapServers
                                + ", and jmx: "
                                + jmxPort.get()
                                + " works well";
                          });
                })
            .build();
    var tab = new Tab("setting");
    tab.setContent(pane);
    return tab;
  }
}
