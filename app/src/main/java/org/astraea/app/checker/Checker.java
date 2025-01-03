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
package org.astraea.app.checker;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Node;
import org.astraea.app.argument.IntegerMapField;
import org.astraea.app.argument.NonEmptyStringField;
import org.astraea.app.argument.NonNegativeIntegerField;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
import org.astraea.common.metrics.JndiClient;
import org.astraea.common.metrics.MBeanClient;

public class Checker {

  private static final List<Guard> GUARDS = List.of(new RpcGuard(), new ConfigGuard());

  public static void main(String[] args) throws Exception {
    execute(Argument.parse(new Argument(), args));
  }

  public static void execute(final Argument param) throws Exception {
    try (var admin = Admin.create(Map.of("bootstrap.servers", param.bootstrapServers()))) {
      for (var guard : GUARDS) {
        var result = guard.run(admin, param.mBeanClientFunction(), param.readChangelog());
        result.forEach(System.out::println);
      }
    }
  }

  public static class Argument extends org.astraea.app.argument.Argument {
    @Parameter(
        names = {"--changelog"},
        description = "String: url of changelog file",
        validateWith = NonEmptyStringField.class)
    String changelog =
        "https://raw.githubusercontent.com/opensource4you/astraea/refs/heads/main/config/kafka_changelog.json";

    Changelog readChangelog() throws IOException {
      try (var in = new URL(changelog).openStream()) {
        return JsonConverter.defaultConverter()
            .fromJson(
                new String(in.readAllBytes(), StandardCharsets.UTF_8), TypeRef.of(Changelog.class));
      }
    }

    @Parameter(
        names = {"--jmx.port"},
        description = "Integer: the port to query JMX for each server",
        validateWith = NonNegativeIntegerField.class,
        converter = NonNegativeIntegerField.class)
    int jmxPort = -1;

    @Parameter(
        names = {"--jmx.ports"},
        description =
            "Map: the JMX port for each broker. For example: 1024=19999 means for the broker with id 1024, its JMX port located at 19999 port",
        validateWith = IntegerMapField.class,
        converter = IntegerMapField.class)
    Map<Integer, Integer> jmxPorts = Map.of();

    Function<Node, MBeanClient> mBeanClientFunction() {
      return node -> {
        int port = jmxPorts.getOrDefault(node.id(), jmxPort);
        if (port < 0)
          throw new IllegalArgumentException("Failed to get jmx port for broker: " + node);
        return JndiClient.of(node.host(), port);
      };
    }
  }
}
