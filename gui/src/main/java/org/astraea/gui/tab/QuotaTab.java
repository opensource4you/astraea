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

import java.util.stream.Collectors;
import org.astraea.common.DataSize;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.Quota;
import org.astraea.common.admin.QuotaConfigs;
import org.astraea.gui.Context;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;

public class QuotaTab {

  private enum Target {
    IP("ip"),
    CLIENT_ID("client id");

    private final String display;

    Target(String display) {
      this.display = display;
    }

    @Override
    public String toString() {
      return display;
    }
  }

  static LinkedHashMap<String, Object> result(Quota quota) {
    return LinkedHashMap.of(
        quota.targetKey(),
        quota.targetValue(),
        quota.limitKey(),
        quota.limitKey().equals(QuotaConfigs.PRODUCER_BYTE_RATE_CONFIG)
                || quota.limitKey().equals(QuotaConfigs.CONSUMER_BYTE_RATE_CONFIG)
            ? DataSize.Byte.of((long) quota.limitValue())
            : quota.limitValue());
  }

  public static Tab of(Context context) {
    return Tab.of(
        "quota",
        PaneBuilder.of()
            .radioButtons(Target.values())
            .searchField("ip/client id")
            .buttonAction(
                (input, logger) -> {
                  var target = input.selectedRadio().map(o -> (Target) o).orElse(Target.IP);
                  return context
                      .admin()
                      .quotas(target == Target.IP ? QuotaConfigs.IP : QuotaConfigs.CLIENT_ID)
                      .thenApply(
                          quotas ->
                              quotas.stream()
                                  .filter(q -> input.matchSearch(q.targetValue()))
                                  .map(QuotaTab::result)
                                  .collect(Collectors.toList()));
                })
            .build());
  }
}
