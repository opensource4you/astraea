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
package org.astraea.app.web;

import com.beust.jcommander.Parameter;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.argument.Field;
import org.astraea.common.argument.NonNegativeIntegerField;

public class WebService {

  public static void main(String[] args) throws Exception {
    execute(org.astraea.common.argument.Argument.parse(new Argument(), args));
  }

  private static void execute(Argument arg) throws IOException {
    var server = HttpServer.create(new InetSocketAddress(arg.port), 0);
    server.createContext("/topics", to(new TopicHandler(AsyncAdmin.of(arg.configs()))));
    server.createContext("/groups", to(new GroupHandler(AsyncAdmin.of(arg.configs()))));
    server.createContext("/brokers", to(new BrokerHandler(AsyncAdmin.of(arg.configs()))));
    server.createContext("/producers", to(new ProducerHandler(AsyncAdmin.of(arg.configs()))));
    server.createContext("/quotas", to(new QuotaHandler(AsyncAdmin.of(arg.configs()))));
    server.createContext("/transactions", to(new TransactionHandler(Admin.of(arg.configs()))));
    if (arg.needJmx())
      server.createContext(
          "/beans", to(new BeanHandler(AsyncAdmin.of(arg.configs()), arg.jmxPorts())));
    server.createContext(
        "/records", to(new RecordHandler(Admin.of(arg.configs()), arg.bootstrapServers())));
    server.createContext("/reassignments", to(new ReassignmentHandler(Admin.of(arg.configs()))));
    server.createContext(
        "/balancer", to(new BalancerHandler(Admin.of(arg.configs()), arg.brokerJmxMap)));
    server.createContext("/throttles", to(new ThrottleHandler(Admin.of(arg.configs()))));
    server.start();
  }

  private static HttpHandler to(Handler handler) {
    return exchange -> handler.handle(Channel.of(exchange));
  }

  static class Argument extends org.astraea.common.argument.Argument {
    @Parameter(
        names = {"--port"},
        description = "Integer: the port to bind",
        validateWith = NonNegativeIntegerField.class,
        converter = NonNegativeIntegerField.class)
    int port = 8001;

    @Parameter(
        names = {"--jmx.port"},
        description = "Integer: the port to query JMX for each server",
        validateWith = NonNegativeIntegerField.class,
        converter = NonNegativeIntegerField.class)
    int jmxPort = -1;

    @Parameter(
        names = {"--broker.jmx"},
        description =
            "Map: the jmx socket address for each node. For example: 1024=192.168.50.2:9875",
        converter = BrokerJmxConverter.class)
    Map<Integer, InetSocketAddress> brokerJmxMap = Map.of();

    boolean needJmx() {
      return jmxPort > 0 || !brokerJmxMap.isEmpty();
    }

    Function<String, Integer> jmxPorts() {
      return name ->
          Optional.of(
                  brokerJmxMap.values().stream()
                      .filter(x -> x.getHostName().equals(name))
                      .map(InetSocketAddress::getPort)
                      .findFirst()
                      .orElse(jmxPort))
              .filter(i -> i > 0)
              .orElseThrow(() -> new NoSuchElementException(name + " has no jmx port"));
    }

    private static class BrokerJmxConverter extends Field<Map<Integer, InetSocketAddress>> {

      private InetSocketAddress asSocketAddress(String string) {
        String[] split = string.split(":");
        return InetSocketAddress.createUnresolved(split[0], Integer.parseInt(split[1]));
      }

      @Override
      public Map<Integer, InetSocketAddress> convert(String value) {
        return Arrays.stream(value.split(","))
            .map(x -> x.split("="))
            .map(x -> Map.entry(Integer.parseInt(x[0]), asSocketAddress(x[1])))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      }
    }
  }
}
