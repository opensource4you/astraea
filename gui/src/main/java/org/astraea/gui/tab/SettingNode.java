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
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javafx.scene.Node;
import org.astraea.common.FutureUtils;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
import org.astraea.gui.Context;
import org.astraea.gui.pane.MultiInput;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.TextInput;

public class SettingNode {

  private static final JsonConverter JSON_CONVERTER = JsonConverter.defaultConverter();

  static class Prop {
    String bootstrapServers;
    Optional<Integer> brokerJmxPort = Optional.empty();
    Optional<String> workerUrl = Optional.empty();
  }

  private static final String BOOTSTRAP_SERVERS = "bootstrap servers";
  private static final String BROKER_JMX_PORT = "broker jmx port";

  private static final String WORKER_URL = "worker url";

  private static Optional<File> propertyFile() {
    var tempDir = System.getProperty("java.io.tmpdir");
    if (tempDir == null) return Optional.empty();
    var dir = new File(tempDir);
    if (!dir.exists() || !dir.isDirectory()) return Optional.empty();
    var f = new File(dir, "astraee_gui_json.properties");
    if (!f.exists() && !Utils.packException(f::createNewFile)) return Optional.empty();
    return Optional.of(f);
  }

  static Optional<Prop> load() {
    return propertyFile()
        .map(
            f -> {
              try (var input = new FileInputStream(f)) {
                var bytes = input.readAllBytes();
                if (bytes == null || bytes.length == 0) return Optional.<Prop>empty();
                return Optional.of(
                    JSON_CONVERTER.fromJson(
                        new String(bytes, StandardCharsets.UTF_8), TypeRef.of(Prop.class)));
              } catch (Exception ignored) {
                return Optional.<Prop>empty();
              }
            })
        .orElse(Optional.empty());
  }

  static void save(Prop prop) {
    propertyFile()
        .ifPresent(
            f -> {
              try (var output = new FileOutputStream(f)) {
                output.write(JSON_CONVERTER.toJson(prop).getBytes(StandardCharsets.UTF_8));
              } catch (IOException ignored) {
                // swallow
              }
            });
  }

  public static Node of(Context context) {
    var properties = load();
    var multiInput =
        MultiInput.of(
            List.of(
                TextInput.required(
                    BOOTSTRAP_SERVERS,
                    EditableText.singleLine()
                        .defaultValue(properties.map(p -> p.bootstrapServers).orElse(null))
                        .disallowEmpty()
                        .build()),
                TextInput.of(
                    BROKER_JMX_PORT,
                    EditableText.singleLine()
                        .onlyNumber()
                        .defaultValue(
                            properties
                                .flatMap(p -> p.brokerJmxPort)
                                .map(String::valueOf)
                                .orElse(null))
                        .build()),
                TextInput.of(
                    WORKER_URL,
                    EditableText.singleLine()
                        .defaultValue(properties.flatMap(p -> p.workerUrl).orElse(null))
                        .disallowEmpty()
                        .build())));
    return PaneBuilder.of()
        .firstPart(
            multiInput,
            "CHECK",
            (argument, logger) -> {
              var prop = new Prop();
              prop.bootstrapServers = argument.nonEmptyTexts().get(BOOTSTRAP_SERVERS);
              prop.brokerJmxPort =
                  Optional.ofNullable(argument.nonEmptyTexts().get(BROKER_JMX_PORT))
                      .map(Integer::parseInt);
              prop.workerUrl = Optional.ofNullable(argument.nonEmptyTexts().get(WORKER_URL));
              save(prop);
              var newAdmin = Admin.of(prop.bootstrapServers);
              var client =
                  prop.workerUrl.map(
                      url ->
                          ConnectorClient.builder()
                              .url(Utils.packException(() -> new URL("http://" + url)))
                              .build());
              return FutureUtils.combine(
                  newAdmin.nodeInfos(),
                  client
                      .map(ConnectorClient::plugins)
                      .orElse(CompletableFuture.completedFuture(Set.of())),
                  (nodeInfos, plugins) -> {
                    context.replace(newAdmin);
                    client.ifPresent(context::replace);
                    if (prop.brokerJmxPort.isEmpty()) {
                      logger.log("succeed to connect to " + prop.bootstrapServers);
                      return List.of();
                    }
                    context.replace(nodeInfos, prop.brokerJmxPort.get());
                    logger.log(
                        "succeed to connect to "
                            + prop.bootstrapServers
                            + prop.workerUrl.map(url -> " and " + url).orElse("")
                            + ". Also, jmx: "
                            + prop.brokerJmxPort.get()
                            + " works well");
                    return List.of();
                  });
            })
        .build();
  }
}
