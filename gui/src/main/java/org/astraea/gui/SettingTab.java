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
import java.util.concurrent.CompletableFuture;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashSet;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.metrics.MBeanClient;

public class SettingTab {

  public static Tab of(Context context) {
    var tab = new Tab("setting");
    tab.setContent(
        Utils.form(
            LinkedHashSet.of("bootstrap servers"),
            LinkedHashSet.of("jmx port"),
            (result, console) -> {
              var bootstrapServers = result.get("bootstrap servers");
              if (bootstrapServers == null || bootstrapServers.isBlank())
                return CompletableFuture.failedFuture(
                    new IllegalArgumentException("please define bootstrap servers"));
              var jmxPort = Optional.ofNullable(result.get("jmx port")).map(Integer::parseInt);
              var newAdmin = AsyncAdmin.of(bootstrapServers);
              return newAdmin
                  .nodeInfos()
                  .thenApply(
                      nodeInfos -> {
                        context
                            .replace(newAdmin)
                            .ifPresent(
                                admin -> org.astraea.common.Utils.swallowException(admin::close));
                        if (jmxPort.isEmpty()) return "succeed to connect to " + bootstrapServers;
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
            },
            "CHECK"));
    return tab;
  }
}
