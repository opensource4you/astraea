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
import java.util.Set;
import java.util.concurrent.CompletionStage;
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
import org.astraea.gui.pane.FirstPart;
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
            TextInput.of(ipLabelKey, EditableText.singleLine().disallowEmpty().build()),
            TextInput.of(rateKey, EditableText.singleLine().onlyNumber().build()));
    var firstPart =
        FirstPart.builder()
            .textInputs(multiInput)
            .clickName("ALTER")
            .tableRefresher(
                (argument, logger) -> {
                  var ip = argument.nonEmptyTexts().get(ipLabelKey);
                  var rate = argument.nonEmptyTexts().get(rateKey);
                  final CompletionStage<Void> result;
                  if (ip == null && rate == null)
                    result = context.admin().unsetConnectionQuotas(Set.of());
                  else if (ip != null && rate != null)
                    result =
                        context.admin().setConnectionQuotas(Map.of(ip, Integer.parseInt(rate)));
                  else if (ip != null) result = context.admin().unsetConnectionQuotas(Set.of(ip));
                  else result = context.admin().setConnectionQuota(Integer.parseInt(rate));
                  return result.thenApply(
                      ignored -> {
                        if (ip == null && rate == null)
                          logger.log("succeed to remove default quota");
                        else if (ip != null && rate != null)
                          logger.log("succeed to alter " + rate + " for " + ip);
                        else if (ip != null) logger.log("succeed to remove quota from " + ip);
                        else logger.log("succeed to alter " + rate + " for default quota");
                        return List.of();
                      });
                })
            .build();
    return PaneBuilder.of().firstPart(firstPart).build();
  }

  private static Node producerNode(Context context) {
    var clientIdLabelKey = "kafka client id";
    var byteRateKey = "MB/second";
    var multiInput =
        List.of(
            TextInput.of(clientIdLabelKey, EditableText.singleLine().disallowEmpty().build()),
            TextInput.of(byteRateKey, EditableText.singleLine().onlyNumber().build()));
    var firstPart =
        FirstPart.builder()
            .textInputs(multiInput)
            .clickName("ALTER")
            .tableRefresher(
                (argument, logger) -> {
                  var clientId = argument.nonEmptyTexts().get(clientIdLabelKey);
                  var rate = argument.nonEmptyTexts().get(byteRateKey);
                  final CompletionStage<Void> result;
                  if (clientId == null && rate == null)
                    result = context.admin().unsetProducerQuotas(Set.of());
                  else if (clientId != null && rate != null)
                    result =
                        context
                            .admin()
                            .setProducerQuotas(
                                Map.of(clientId, DataRate.MB.of(Long.parseLong(rate))));
                  else if (clientId != null)
                    result = context.admin().unsetProducerQuotas(Set.of(clientId));
                  else
                    result = context.admin().setProducerQuota(DataRate.MB.of(Long.parseLong(rate)));
                  return result.thenApply(
                      ignored -> {
                        if (clientId == null && rate == null)
                          logger.log("succeed to remove default quota");
                        else if (clientId != null && rate != null)
                          logger.log("succeed to alter " + rate + " for " + clientId);
                        else if (clientId != null)
                          logger.log("succeed to remove quota from " + clientId);
                        else logger.log("succeed to alter " + rate + " for default quota");
                        return List.of();
                      });
                })
            .build();
    return PaneBuilder.of().firstPart(firstPart).build();
  }

  private static Node consumerNode(Context context) {
    var clientIdLabelKey = "kafka client id";
    var byteRateKey = "MB/second";
    var multiInput =
        List.of(
            TextInput.of(clientIdLabelKey, EditableText.singleLine().disallowEmpty().build()),
            TextInput.of(byteRateKey, EditableText.singleLine().onlyNumber().build()));
    var firstPart =
        FirstPart.builder()
            .textInputs(multiInput)
            .clickName("ALTER")
            .tableRefresher(
                (argument, logger) -> {
                  var clientId = argument.nonEmptyTexts().get(clientIdLabelKey);
                  var rate = argument.nonEmptyTexts().get(byteRateKey);
                  final CompletionStage<Void> result;
                  if (clientId == null && rate == null)
                    result = context.admin().unsetConsumerQuotas(Set.of());
                  else if (clientId != null && rate != null)
                    result =
                        context
                            .admin()
                            .setConsumerQuotas(
                                Map.of(clientId, DataRate.MB.of(Long.parseLong(rate))));
                  else if (clientId != null)
                    result = context.admin().unsetConsumerQuotas(Set.of(clientId));
                  else
                    result = context.admin().setConsumerQuota(DataRate.MB.of(Long.parseLong(rate)));
                  return result.thenApply(
                      ignored -> {
                        if (clientId == null && rate == null)
                          logger.log("succeed to remove default quota");
                        else if (clientId != null && rate != null)
                          logger.log("succeed to alter " + rate + " for " + clientId);
                        else if (clientId != null)
                          logger.log("succeed to remove quota from " + clientId);
                        else logger.log("succeed to alter " + rate + " for default quota");
                        return List.of();
                      });
                })
            .build();
    return PaneBuilder.of().firstPart(firstPart).build();
  }

  static LinkedHashMap<String, Object> basicResult(Quota quota) {
    return MapUtils.of(
        "entity",
        quota.targetKey(),
        "name",
        quota.targetValue() == null ? "<default>" : quota.targetValue(),
        "limit",
        quota.limitKey(),
        "value",
        quota.limitKey().equals(QuotaConfigs.PRODUCER_BYTE_RATE_CONFIG)
                || quota.limitKey().equals(QuotaConfigs.CONSUMER_BYTE_RATE_CONFIG)
            ? DataSize.Byte.of((long) quota.limitValue())
            : quota.limitValue());
  }

  private static Node basicNode(Context context) {
    var firstPart =
        FirstPart.builder()
            .clickName("REFRESH")
            .tableRefresher(
                (argument, logger) ->
                    FutureUtils.combine(
                        context.admin().quotas(Set.of(QuotaConfigs.IP)),
                        context.admin().quotas(Set.of(QuotaConfigs.CLIENT_ID)),
                        (q0, q1) ->
                            Stream.concat(q0.stream(), q1.stream())
                                .map(QuotaNode::basicResult)
                                .collect(Collectors.toList())))
            .build();
    return PaneBuilder.of().firstPart(firstPart).build();
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
