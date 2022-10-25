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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.geometry.Side;
import javafx.scene.layout.Pane;
import org.astraea.common.DataRate;
import org.astraea.common.DataSize;
import org.astraea.common.FutureUtils;
import org.astraea.common.MapUtils;
import org.astraea.common.admin.Quota;
import org.astraea.common.admin.QuotaConfigs;
import org.astraea.gui.Context;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;
import org.astraea.gui.pane.TabPane;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.NoneditableText;

public class QuotaTab {

  private static Pane connectionPane(Context context) {
    var ipLabelKey = "ip address";
    var rateKey = "connections/second";
    return PaneBuilder.of()
        .buttonName("ALTER")
        .input(
            NoneditableText.highlight(ipLabelKey),
            EditableText.singleLine().disallowEmpty().build())
        .input(NoneditableText.of(rateKey), EditableText.singleLine().onlyNumber().build())
        .buttonAction(
            (input, logger) ->
                Optional.ofNullable(input.nonEmptyTexts().get(rateKey))
                    .map(
                        rate ->
                            context
                                .admin()
                                .setConnectionQuotas(
                                    Map.of(
                                        input.nonEmptyTexts().get(ipLabelKey),
                                        Integer.parseInt(rate))))
                    .orElseGet(
                        () ->
                            context
                                .admin()
                                .unsetConnectionQuotas(
                                    Set.of(input.nonEmptyTexts().get(ipLabelKey))))
                    .thenCompose(
                        ignored ->
                            context
                                .admin()
                                .quotas(Set.of(QuotaConfigs.IP))
                                .thenApply(
                                    quotas ->
                                        quotas.stream()
                                            .map(QuotaTab::basicResult)
                                            .collect(Collectors.toList()))))
        .build();
  }

  private static Pane producerPane(Context context) {
    var clientIdLabelKey = "kafka client id";
    var byteRateKey = "MB/second";
    return PaneBuilder.of()
        .buttonName("ALTER")
        .input(
            NoneditableText.highlight(clientIdLabelKey),
            EditableText.singleLine().disallowEmpty().build())
        .input(NoneditableText.of(byteRateKey), EditableText.singleLine().onlyNumber().build())
        .buttonAction(
            (input, logger) ->
                Optional.ofNullable(input.nonEmptyTexts().get(byteRateKey))
                    .map(
                        rate ->
                            context
                                .admin()
                                .setProducerQuotas(
                                    Map.of(
                                        input.nonEmptyTexts().get(clientIdLabelKey),
                                        DataRate.MB.of(Long.parseLong(rate)).perSecond())))
                    .orElseGet(
                        () ->
                            context
                                .admin()
                                .unsetProducerQuotas(
                                    Set.of(input.nonEmptyTexts().get(clientIdLabelKey))))
                    .thenCompose(
                        ignored ->
                            context
                                .admin()
                                .quotas(Set.of(QuotaConfigs.CLIENT_ID))
                                .thenApply(
                                    quotas ->
                                        quotas.stream()
                                            .map(QuotaTab::basicResult)
                                            .collect(Collectors.toList()))))
        .build();
  }

  private static Pane consumerPane(Context context) {
    var clientIdLabelKey = "kafka client id";
    var byteRateKey = "MB/second";
    return PaneBuilder.of()
        .buttonName("ALTER")
        .input(
            NoneditableText.highlight(clientIdLabelKey),
            EditableText.singleLine().disallowEmpty().build())
        .input(NoneditableText.of(byteRateKey), EditableText.singleLine().onlyNumber().build())
        .buttonAction(
            (input, logger) ->
                Optional.ofNullable(input.nonEmptyTexts().get(byteRateKey))
                    .map(
                        rate ->
                            context
                                .admin()
                                .setConsumerQuotas(
                                    Map.of(
                                        input.nonEmptyTexts().get(clientIdLabelKey),
                                        DataRate.MB.of(Long.parseLong(rate)).perSecond())))
                    .orElseGet(
                        () ->
                            context
                                .admin()
                                .unsetConsumerQuotas(
                                    Set.of(input.nonEmptyTexts().get(clientIdLabelKey))))
                    .thenCompose(
                        ignored ->
                            context
                                .admin()
                                .quotas(Set.of(QuotaConfigs.CLIENT_ID))
                                .thenApply(
                                    quotas ->
                                        quotas.stream()
                                            .map(QuotaTab::basicResult)
                                            .collect(Collectors.toList()))))
        .build();
  }

  public static Tab alterTab(Context context) {
    return Tab.of(
        "alter",
        TabPane.of(
            Side.TOP,
            Map.of(
                "connection",
                connectionPane(context),
                "producer",
                producerPane(context),
                "consumer",
                consumerPane(context))));
  }

  static LinkedHashMap<String, Object> basicResult(Quota quota) {
    return MapUtils.of(
        quota.targetKey(),
        quota.targetValue(),
        quota.limitKey(),
        quota.limitKey().equals(QuotaConfigs.PRODUCER_BYTE_RATE_CONFIG)
                || quota.limitKey().equals(QuotaConfigs.CONSUMER_BYTE_RATE_CONFIG)
            ? DataSize.Byte.of((long) quota.limitValue())
            : quota.limitValue());
  }

  private static Tab basicTab(Context context) {
    return Tab.of(
        "basic",
        PaneBuilder.of()
            .buttonAction(
                (input, logger) ->
                    FutureUtils.combine(
                        context.admin().quotas(Set.of(QuotaConfigs.IP)),
                        context.admin().quotas(Set.of(QuotaConfigs.CLIENT_ID)),
                        (q0, q1) ->
                            Stream.concat(q0.stream(), q1.stream())
                                .map(QuotaTab::basicResult)
                                .collect(Collectors.toList())))
            .build());
  }

  public static Tab of(Context context) {
    return Tab.of("quota", TabPane.of(Side.TOP, List.of(basicTab(context), alterTab(context))));
  }
}
