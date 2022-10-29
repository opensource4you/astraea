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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import javafx.scene.Node;
import org.astraea.common.Utils;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.gui.Context;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.NoneditableText;

public class SettingNode {

  private static final String BOOTSTRAP_SERVERS = "bootstrap servers";
  private static final String JMX_PORT = "jmx port";

  private static Optional<File> propertyFile() {
    var tempDir = System.getProperty("java.io.tmpdir");
    if (tempDir == null) return Optional.empty();
    var dir = new File(tempDir);
    if (!dir.exists() || !dir.isDirectory()) return Optional.empty();
    var f = new File(dir, "astraee_gui.properties");
    if (!f.exists() && !Utils.packException(f::createNewFile)) return Optional.empty();
    return Optional.of(f);
  }

  private static Map<String, String> loadProperty() {
    return propertyFile()
        .map(
            f -> {
              try (var input = new FileInputStream(f)) {
                var props = new Properties();
                props.load(input);
                return props.entrySet().stream()
                    .filter(
                        e -> !e.getKey().toString().isBlank() && !e.getValue().toString().isBlank())
                    .collect(
                        Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
              } catch (IOException e) {
                return Map.<String, String>of();
              }
            })
        .orElse(Map.of());
  }

  private static void saveProperty(Map<String, String> configs) {
    propertyFile()
        .ifPresent(
            f -> {
              try (var output = new FileOutputStream(f)) {
                var props = new Properties();
                props.putAll(configs);
                props.store(output, null);
              } catch (IOException e) {
                // swallow
              }
            });
  }

  public static Node of(Context context) {
    var bootstrapKey = "bootstrap";
    var jmxPortKey = "jmx";
    var properties = loadProperty();
    return PaneBuilder.of()
        .input(
            NoneditableText.highlight(BOOTSTRAP_SERVERS),
            EditableText.singleLine()
                .defaultValue(properties.get(bootstrapKey))
                .disallowEmpty()
                .build())
        .input(
            NoneditableText.of(JMX_PORT),
            EditableText.singleLine().onlyNumber().defaultValue(properties.get(jmxPortKey)).build())
        .clickName("CHECK")
        .clickListener(
            (input, logger) -> {
              var bootstrapServers = input.nonEmptyTexts().get(BOOTSTRAP_SERVERS);
              Objects.requireNonNull(bootstrapServers);
              var jmxPort =
                  Optional.ofNullable(input.nonEmptyTexts().get(JMX_PORT)).map(Integer::parseInt);
              saveProperty(
                  Map.of(
                      bootstrapKey,
                      bootstrapServers,
                      jmxPortKey,
                      jmxPort.map(String::valueOf).orElse("")));
              var newAdmin = AsyncAdmin.of(bootstrapServers);
              return newAdmin
                  .nodeInfos()
                  .thenAccept(
                      nodeInfos -> {
                        context.replace(newAdmin);
                        if (jmxPort.isEmpty()) {
                          logger.log("succeed to connect to " + bootstrapServers);
                          return;
                        }
                        context.replace(nodeInfos, jmxPort.get());
                        logger.log(
                            "succeed to connect to "
                                + bootstrapServers
                                + ", and jmx: "
                                + jmxPort.get()
                                + " works well");
                      });
            })
        .build();
  }
}
