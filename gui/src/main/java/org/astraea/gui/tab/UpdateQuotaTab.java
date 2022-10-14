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

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import javafx.scene.layout.Pane;
import org.astraea.common.DataRate;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.admin.QuotaConfigs;
import org.astraea.gui.Context;
import org.astraea.gui.pane.BorderPane;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;

public class UpdateQuotaTab {

  private static final String IP_LABEL_KEY = "ip address";
  private static final String CLIENT_ID_LABEL_KEY = "kafka client id";

  private static final String RATE_KEY = "connections/second";
  private static final String BYTE_RATE_KEY = "MB/second";

  private enum Limit {
    CONNECTION("connection"),
    PRODUCER("producer"),
    CONSUMER("consumer");

    private final String display;

    Limit(String display) {
      this.display = display;
    }

    @Override
    public String toString() {
      return display;
    }
  }

  private static Pane pane(AsyncAdmin admin, Limit limit) {
    return PaneBuilder.of()
        .buttonName("UPDATE")
        .input(limit == Limit.CONNECTION ? IP_LABEL_KEY : CLIENT_ID_LABEL_KEY, true, false)
        .input(limit == Limit.CONNECTION ? RATE_KEY : BYTE_RATE_KEY, false, true)
        .buttonAction(
            (input, logger) -> {
              CompletionStage<Void> result;
              switch (limit) {
                case CONNECTION:
                  result =
                      Optional.ofNullable(input.nonEmptyTexts().get(RATE_KEY))
                          .map(
                              rate ->
                                  admin.setConnectionQuotas(
                                      Map.of(
                                          input.nonEmptyTexts().get(IP_LABEL_KEY),
                                          Integer.parseInt(rate))))
                          .orElseGet(
                              () ->
                                  admin.unsetConnectionQuotas(
                                      Set.of(input.nonEmptyTexts().get(IP_LABEL_KEY))));
                  break;
                case PRODUCER:
                  result =
                      Optional.ofNullable(input.nonEmptyTexts().get(BYTE_RATE_KEY))
                          .map(
                              rate ->
                                  admin.setProducerQuotas(
                                      Map.of(
                                          input.nonEmptyTexts().get(CLIENT_ID_LABEL_KEY),
                                          DataRate.MB.of(Long.parseLong(rate)).perSecond())))
                          .orElseGet(
                              () ->
                                  admin.unsetProducerQuotas(
                                      Set.of(input.nonEmptyTexts().get(CLIENT_ID_LABEL_KEY))));
                  break;
                case CONSUMER:
                  result =
                      Optional.ofNullable(input.nonEmptyTexts().get(BYTE_RATE_KEY))
                          .map(
                              rate ->
                                  admin.setConsumerQuotas(
                                      Map.of(
                                          input.nonEmptyTexts().get(CLIENT_ID_LABEL_KEY),
                                          DataRate.MB.of(Long.parseLong(rate)).perSecond())))
                          .orElseGet(
                              () ->
                                  admin.unsetConsumerQuotas(
                                      Set.of(input.nonEmptyTexts().get(CLIENT_ID_LABEL_KEY))));
                  break;
                default:
                  result = CompletableFuture.completedFuture(null);
                  break;
              }
              return result.thenCompose(
                  ignored ->
                      admin
                          .quotas(
                              limit == Limit.CONNECTION ? QuotaConfigs.IP : QuotaConfigs.CLIENT_ID)
                          .thenApply(
                              quotas ->
                                  quotas.stream()
                                      .map(QuotaTab::result)
                                      .collect(Collectors.toList())));
            })
        .build();
  }

  public static Tab of(Context context) {
    return Tab.of(
        "update quota",
        BorderPane.dynamic(
            Arrays.stream(Limit.values()).collect(Collectors.toSet()),
            limit ->
                context.submit(admin -> CompletableFuture.completedFuture(pane(admin, limit)))));
  }
}
