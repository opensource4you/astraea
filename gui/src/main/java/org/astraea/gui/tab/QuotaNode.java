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
import javafx.scene.Node;
import org.astraea.common.DataRate;
import org.astraea.common.DataSize;
import org.astraea.common.FutureUtils;
import org.astraea.common.MapUtils;
import org.astraea.common.admin.Quota;
import org.astraea.common.admin.QuotaConfigs;
import org.astraea.gui.Context;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Slide;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.TextInput;

public class QuotaNode {

  private static Node connectionNode(Context context) {
    var ipLabelKey = "ip address";
    var rateKey = "connections/second";
    var multiInput =
        List.of(
            TextInput.required(ipLabelKey, EditableText.singleLine().disallowEmpty().build()),
            TextInput.of(rateKey, EditableText.singleLine().onlyNumber().build()));
    return PaneBuilder.of()
        .firstPart(
            multiInput,
            "ALTER",
            (argument, logger) ->
                Optional.ofNullable(argument.nonEmptyTexts().get(rateKey))
                    .map(
                        rate ->
                            context
                                .admin()
                                .setConnectionQuotas(
                                    Map.of(
                                        argument.nonEmptyTexts().get(ipLabelKey),
                                        Integer.parseInt(rate))))
                    .orElseGet(
                        () ->
                            context
                                .admin()
                                .unsetConnectionQuotas(
                                    Set.of(argument.nonEmptyTexts().get(ipLabelKey))))
                    .thenApply(
                        ignored -> {
                          logger.log(
                              "succeed to alter rate for "
                                  + argument.nonEmptyTexts().get(ipLabelKey));
                          return List.of();
                        }))
        .build();
  }

  private static Node producerNode(Context context) {
    var clientIdLabelKey = "kafka client id";
    var byteRateKey = "MB/second";
    var multiInput =
        List.of(
            TextInput.required(clientIdLabelKey, EditableText.singleLine().disallowEmpty().build()),
            TextInput.of(byteRateKey, EditableText.singleLine().onlyNumber().build()));
    return PaneBuilder.of()
        .firstPart(
            multiInput,
            "ALTER",
            (argument, logger) ->
                Optional.ofNullable(argument.nonEmptyTexts().get(byteRateKey))
                    .map(
                        rate ->
                            context
                                .admin()
                                .setProducerQuotas(
                                    Map.of(
                                        argument.nonEmptyTexts().get(clientIdLabelKey),
                                        DataRate.MB.of(Long.parseLong(rate)).perSecond())))
                    .orElseGet(
                        () ->
                            context
                                .admin()
                                .unsetProducerQuotas(
                                    Set.of(argument.nonEmptyTexts().get(clientIdLabelKey))))
                    .thenApply(
                        ignored -> {
                          logger.log(
                              "succeed to alter rate for "
                                  + argument.nonEmptyTexts().get(clientIdLabelKey));
                          return List.of();
                        }))
        .build();
  }

  private static Node consumerNode(Context context) {
    var clientIdLabelKey = "kafka client id";
    var byteRateKey = "MB/second";
    var multiInput =
        List.of(
            TextInput.required(clientIdLabelKey, EditableText.singleLine().disallowEmpty().build()),
            TextInput.of(byteRateKey, EditableText.singleLine().onlyNumber().build()));
    return PaneBuilder.of()
        .firstPart(
            multiInput,
            "ALTER",
            (argument, logger) ->
                Optional.ofNullable(argument.nonEmptyTexts().get(byteRateKey))
                    .map(
                        rate ->
                            context
                                .admin()
                                .setConsumerQuotas(
                                    Map.of(
                                        argument.nonEmptyTexts().get(clientIdLabelKey),
                                        DataRate.MB.of(Long.parseLong(rate)).perSecond())))
                    .orElseGet(
                        () ->
                            context
                                .admin()
                                .unsetConsumerQuotas(
                                    Set.of(argument.nonEmptyTexts().get(clientIdLabelKey))))
                    .thenApply(
                        ignored -> {
                          logger.log(
                              "succeed to alter rate for "
                                  + argument.nonEmptyTexts().get(clientIdLabelKey));
                          return List.of();
                        }))
        .build();
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

  private static Node basicNode(Context context) {
    return PaneBuilder.of()
        .firstPart(
            "REFRESH",
            (argument, logger) ->
                FutureUtils.combine(
                    context.admin().quotas(Set.of(QuotaConfigs.IP)),
                    context.admin().quotas(Set.of(QuotaConfigs.CLIENT_ID)),
                    (q0, q1) ->
                        Stream.concat(q0.stream(), q1.stream())
                            .map(QuotaNode::basicResult)
                            .collect(Collectors.toList())))
        .build();
  }

  public static Node of(Context context) {
    return Slide.of(
            Side.TOP,
            MapUtils.of(
                "basic",
                basicNode(context),
                "connection",
                connectionNode(context),
                "producer",
                producerNode(context),
                "consumer",
                consumerNode(context)))
        .node();
  }
}
