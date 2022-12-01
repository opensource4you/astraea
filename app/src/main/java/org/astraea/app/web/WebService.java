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
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.argument.DurationField;
import org.astraea.common.argument.NonNegativeIntegerField;
import org.astraea.common.argument.StringMapField;

public class WebService implements AutoCloseable {

  private final HttpServer server;
  private final Admin admin;

  public WebService(Admin admin, int port, Function<Integer, Optional<Integer>> brokerIdToJmxPort) {
    this.admin = admin;
    server = Utils.packException(() -> HttpServer.create(new InetSocketAddress(port), 0));
    server.createContext("/topics", to(new TopicHandler(admin)));
    server.createContext("/groups", to(new GroupHandler(admin)));
    server.createContext("/brokers", to(new BrokerHandler(admin)));
    server.createContext("/producers", to(new ProducerHandler(admin)));
    server.createContext("/quotas", to(new QuotaHandler(admin)));
    server.createContext("/transactions", to(new TransactionHandler(admin)));
    server.createContext("/beans", to(new BeanHandler(admin, brokerIdToJmxPort)));
    server.createContext("/records", to(new RecordHandler(admin)));
    server.createContext("/reassignments", to(new ReassignmentHandler(admin)));
    server.createContext("/balancer", to(new BalancerHandler(admin, brokerIdToJmxPort)));
    server.createContext("/throttles", to(new ThrottleHandler(admin)));
    server.start();
  }

  public int port() {
    return server.getAddress().getPort();
  }

  @Override
  public void close() {
    Utils.swallowException(admin::close);
    server.stop(3);
  }

  public static void main(String[] args) throws Exception {
    var arg = org.astraea.common.argument.Argument.parse(new Argument(), args);
    Function<Integer, Optional<Integer>> brokerIdToPort =
        id -> {
          var r = Optional.ofNullable(arg.jmxPorts.get(String.valueOf(id))).map(Integer::parseInt);
          if (r.isPresent()) return r;
          if (arg.jmxPort > 0) return Optional.of(arg.jmxPort);
          return Optional.empty();
        };
    try (var service = new WebService(Admin.of(arg.configs()), arg.port, brokerIdToPort)) {
      if (arg.ttl == null) {
        System.out.println("enter ctrl + c to terminate web service");
        TimeUnit.MILLISECONDS.sleep(Long.MAX_VALUE);
      } else {
        System.out.println(
            "this web service will get terminated automatically after "
                + arg.ttl.toSeconds()
                + " seconds");
        TimeUnit.MILLISECONDS.sleep(arg.ttl.toMillis());
      }
    }
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
        names = {"--jmx.ports"},
        description =
            "Map: the JMX port for each broker. For example: 1024=19999 means for the broker with id 1024, its JMX port located at 19999 port",
        validateWith = StringMapField.class,
        converter = StringMapField.class)
    Map<String, String> jmxPorts = Map.of();

    @Parameter(
        names = {"--ttl"},
        description = "Duration: the life of web service",
        validateWith = DurationField.class,
        converter = DurationField.class)
    Duration ttl = null;
  }
}
