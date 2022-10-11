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

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import org.astraea.common.admin.BrokerConfigs;
import org.astraea.gui.Context;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;

public class UpdateBrokerTab {

  private static final String BROKER_ID = "broker id";

  public static Tab of(Context context) {

    var pane =
        PaneBuilder.of()
            .buttonName("UPDATE")
            .input(BROKER_ID, true, true)
            .input(BrokerConfigs.DYNAMICAL_CONFIGS)
            .buttonListener(
                (input, logger) -> {
                  var allConfigs = new HashMap<>(input.texts());
                  var id = Integer.parseInt(allConfigs.remove(BROKER_ID));
                  return context.submit(
                      admin ->
                          admin
                              .brokers()
                              .thenCompose(
                                  brokers -> {
                                    if (brokers.stream().noneMatch(b -> b.id() == id))
                                      return CompletableFuture.failedFuture(
                                          new IllegalArgumentException(
                                              "broker:" + id + " is nonexistent"));
                                    return admin
                                        .updateConfig(id, allConfigs)
                                        .thenAccept(
                                            ignored -> logger.log("succeed to update " + id));
                                  }));
                })
            .build();
    return Tab.of("update broker", pane);
  }
}
