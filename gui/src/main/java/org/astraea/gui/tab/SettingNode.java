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

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javafx.scene.Node;
import org.astraea.common.FutureUtils;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.connector.WorkerStatus;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
import org.astraea.gui.Context;
import org.astraea.gui.pane.FirstPart;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.TextInput;

public class SettingNode {

  private static final JsonConverter JSON_CONVERTER = JsonConverter.defaultConverter();

  static class Prop {
    String bootstrapServers;
    Optional<Integer> brokerJmxPort = Optional.empty();
    Optional<String> workerUrl = Optional.empty();
    Optional<Integer> workerJmxPort = Optional.empty();
  }

  private static final String BOOTSTRAP_SERVERS = "bootstrap servers";
  private static final String BROKER_JMX_PORT = "broker jmx port";

  private static final String WORKER_URL = "worker url";
  private static final String WORKER_JMX_PORT = "worker jmx port";

  private static Optional<Path> propertyFile() {
    var tempDir = System.getProperty("java.io.tmpdir");
    if (tempDir == null) return Optional.empty();
    var dir = Path.of(tempDir);
    if (!Files.isDirectory(dir)) return Optional.empty();
    var f = dir.resolve("astraee_gui_json.properties");
    if (Files.isRegularFile(f)) return Optional.of(f);
    return Utils.packException(() -> Optional.of(Files.createFile(f)));
  }

  static Optional<Prop> load() {
    return propertyFile()
        .map(
            f -> {
              try (var input = Files.newInputStream(f)) {
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
              try (var output = Files.newOutputStream(f)) {
                output.write(JSON_CONVERTER.toJson(prop).getBytes(StandardCharsets.UTF_8));
              } catch (IOException ignored) {
                // swallow
              }
            });
  }

  public static Node of(Context context) {
    var properties = load();
    var multiInput =
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
                        properties.flatMap(p -> p.brokerJmxPort).map(String::valueOf).orElse(null))
                    .build()),
            TextInput.of(
                WORKER_URL,
                EditableText.singleLine()
                    .defaultValue(properties.flatMap(p -> p.workerUrl).orElse(null))
                    .disallowEmpty()
                    .build()),
            TextInput.of(
                WORKER_JMX_PORT,
                EditableText.singleLine()
                    .onlyNumber()
                    .defaultValue(
                        properties.flatMap(p -> p.workerJmxPort).map(String::valueOf).orElse(null))
                    .build()));
    var firstPart =
        FirstPart.builder()
            .textInputs(multiInput)
            .clickName("CHECK")
            .tableRefresher(
                (argument, logger) -> {
                  var prop = new Prop();
                  prop.bootstrapServers = argument.nonEmptyTexts().get(BOOTSTRAP_SERVERS);
                  prop.brokerJmxPort =
                      Optional.ofNullable(argument.nonEmptyTexts().get(BROKER_JMX_PORT))
                          .map(Integer::parseInt);
                  prop.workerUrl = Optional.ofNullable(argument.nonEmptyTexts().get(WORKER_URL));
                  prop.workerJmxPort =
                      Optional.ofNullable(argument.nonEmptyTexts().get(WORKER_JMX_PORT))
                          .map(Integer::parseInt);
                  save(prop);
                  var newAdmin = Admin.of(prop.bootstrapServers);
                  var client =
                      prop.workerUrl.map(
                          url ->
                              ConnectorClient.builder()
                                  .url(Utils.packException(() -> new URL("http://" + url)))
                                  .build());
                  return FutureUtils.combine(
                      newAdmin.brokers(),
                      client
                          .map(ConnectorClient::activeWorkers)
                          .orElse(CompletableFuture.completedFuture(List.of())),
                      (brokers, workers) -> {
                        context.replace(newAdmin);
                        client.ifPresent(context::replace);
                        prop.brokerJmxPort.ifPresent(context::brokerJmxPort);
                        prop.workerJmxPort.ifPresent(context::workerJmxPort);
                        context.addBrokerClients(brokers);
                        context.addWorkerClients(
                            workers.stream()
                                .map(WorkerStatus::hostname)
                                .collect(Collectors.toSet()));
                        logger.log(
                            "succeed to connect to "
                                + prop.bootstrapServers
                                + prop.workerUrl.map(url -> " and " + url).orElse(""));
                        return List.of();
                      });
                })
            .build();
    return PaneBuilder.of().firstPart(firstPart).build();
  }
}
