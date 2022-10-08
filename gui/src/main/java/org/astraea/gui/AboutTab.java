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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.LinkedHashSet;
import org.astraea.common.VersionUtils;

public class AboutTab {

  private enum Info {
    Version(
        "version",
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
        "author",
        List.of(
            LinkedHashMap.of(
                "name", "Chia-Ping Tsai",
                "email", "chia7712@gmail.com"),
            LinkedHashMap.of(
                "name", "Yi-Chen Wang",
                "email", "warren215215@gmail.com"),
            LinkedHashMap.of(
                "name", "Ching-Hong Fang",
                "email", "fjh7777@gmail.com"),
            LinkedHashMap.of(
                "name", "Zheng-Xian Li",
                "email", "garyparrottt@gmail.com"),
            LinkedHashMap.of(
                "name", "Xiang-Jun Sun",
                "email", "sean0651101@gmail.com"),
            LinkedHashMap.of(
                "name", "Zhi-Mao Teng",
                "email", "zhimao.teng@gmail.com"),
            LinkedHashMap.of(
                "name", "Jia-Sheng Chen",
                "email", "haser1156@gmail.com"),
            LinkedHashMap.of(
                "name", "Chao-Heng Lee",
                "email", "chaohengstudent@gmail.com"),
            LinkedHashMap.of(
                "name", "Yi-Huan Lee",
                "email", "yi.huan.max@gmail.com")));

    private final String alias;

    private final List<Map<String, Object>> tables;

    Info(String alias, List<Map<String, Object>> tables) {
      this.alias = alias;
      this.tables = tables;
    }
  }

  public static Tab of(Context context) {
    var pane =
        PaneBuilder.of()
            .radioButtons(
                LinkedHashSet.of(
                    Arrays.stream(Info.values()).map(c -> c.alias).toArray(String[]::new)))
            .outputTable(
                input ->
                    CompletableFuture.completedFuture(
                        Arrays.stream(Info.values())
                            .filter(
                                info ->
                                    input
                                        .selectedRadio()
                                        .filter(info.alias::equals)
                                        .isPresent())
                            .findFirst()
                            .orElse(Info.Version)
                            .tables))
            .build();
    var tab = new Tab("about");
    tab.setContent(pane);
    return tab;
  }
}
