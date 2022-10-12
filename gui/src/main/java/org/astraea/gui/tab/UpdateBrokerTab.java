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

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import javafx.geometry.Side;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.BrokerConfigs;
import org.astraea.common.admin.NodeInfo;
import org.astraea.gui.Context;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;
import org.astraea.gui.pane.TabPane;

public class UpdateBrokerTab {

  private static List<Tab> brokerTabs(Context context, List<Broker> brokers) {
    return brokers.stream()
        .sorted(Comparator.comparing(NodeInfo::id))
        .map(
            broker ->
                Tab.of(
                    String.valueOf(broker.id()),
                    PaneBuilder.of()
                        .buttonName("UPDATE")
                        .input(
                            BrokerConfigs.DYNAMICAL_CONFIGS.stream()
                                .collect(
                                    Collectors.toMap(
                                        k -> k,
                                        k ->
                                            brokers.stream()
                                                .filter(b -> b.id() == broker.id())
                                                .flatMap(b -> b.config().value(k).stream())
                                                .findFirst()
                                                .orElse(""))))
                        .buttonListener(
                            (input, logger) ->
                                context.submit(
                                    admin ->
                                        admin
                                            .setConfigs(broker.id(), input.nonEmptyTexts())
                                            .thenCompose(
                                                ignored ->
                                                    admin.unsetConfigs(
                                                        broker.id(), input.emptyValueKeys()))
                                            .thenAccept(
                                                ignored ->
                                                    logger.log(
                                                        "succeed to update " + broker.id()))))
                        .build()))
        .collect(Collectors.toList());
  }

  public static Tab of(Context context) {
    return Tab.dynamic(
        "update broker",
        () ->
            context
                .submit(AsyncAdmin::brokers)
                .thenApply(brokers -> TabPane.of(Side.TOP, brokerTabs(context, brokers))));
  }
}
