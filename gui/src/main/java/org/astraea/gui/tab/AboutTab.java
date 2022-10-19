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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.VersionUtils;
import org.astraea.gui.Context;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;

public class AboutTab {

  private enum Info {
    Version(
        "版本",
        List.of(
            LinkedHashMap.of(
                "version",
                VersionUtils.VERSION,
                "revision",
                VersionUtils.REVISION,
                "build date",
                VersionUtils.DATE,
                "web site",
                "https://github.com/skiptests/astraea"))),
    Author(
        "維護者",
        List.of(
            LinkedHashMap.of(
                "name",
                "蔡嘉平",
                "email",
                "chia7712@gmail.com",
                "個人頁面",
                "https://github.com/chia7712"),
            LinkedHashMap.of(
                "name",
                "王懿宸",
                "email",
                "warren215215@gmail.com",
                "個人頁面",
                "https://github.com/wycccccc"),
            LinkedHashMap.of(
                "name",
                "方竫泓",
                "email",
                "fjh7777@gmail.com",
                "個人頁面",
                "https://github.com/chinghongfang"),
            LinkedHashMap.of(
                "name",
                "李政憲",
                "email",
                "garyparrottt@gmail.com",
                "個人頁面",
                "https://github.com/garyparrot"),
            LinkedHashMap.of(
                "name",
                "孫祥鈞",
                "email",
                "sean0651101@gmail.com",
                "個人頁面",
                "https://github.com/qoo332001"),
            LinkedHashMap.of(
                "name",
                "鄧智懋",
                "email",
                "zhimao.teng@gmail.com",
                "個人頁面",
                "https://github.com/harryteng9527"),
            LinkedHashMap.of(
                "name",
                "陳嘉晟",
                "email",
                "haser1156@gmail.com",
                "個人頁面",
                "https://github.com/Haser0305"),
            LinkedHashMap.of(
                "name",
                "李兆恆",
                "email",
                "chaohengstudent@gmail.com",
                "個人頁面",
                "https://github.com/chaohengstudent"),
            LinkedHashMap.of(
                "name",
                "李宜桓",
                "email",
                "yi.huan.max@gmail.com",
                "個人頁面",
                "https://github.com/MaxwellYHL")));

    private final String display;

    private final List<Map<String, Object>> tables;

    Info(String display, List<Map<String, Object>> tables) {
      this.display = display;
      this.tables = tables;
    }

    @Override
    public String toString() {
      return display;
    }
  }

  public static Tab of(Context ignored) {
    var pane =
        PaneBuilder.of()
            .singleRadioButtons(Info.values())
            .buttonAction(
                (input, logger) ->
                    CompletableFuture.completedFuture(
                        input.singleSelectedRadio().map(o -> (Info) o).orElse(Info.Version).tables))
            .build();
    return Tab.of("about", pane);
  }
}
