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
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.app.argument.DurationField;
import org.astraea.app.argument.IntegerMapField;
import org.astraea.app.argument.NonNegativeIntegerField;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.metrics.JndiClient;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.collector.MetricSensor;
import org.astraea.common.metrics.collector.MetricStore;

public class WebService implements AutoCloseable {

  private final HttpServer server;
  private final Admin admin;
  private final Sensors sensors = new Sensors();

  public WebService(
      Admin admin,
      int port,
      Function<Integer, Integer> brokerIdToJmxPort,
      Duration beanExpiration) {
    this.admin = admin;
    Supplier<CompletionStage<Map<Integer, MBeanClient>>> clientSupplier =
        () ->
            admin
                .brokers()
                .thenApply(
                    brokers ->
                        brokers.stream()
                            .collect(
                                Collectors.toUnmodifiableMap(
                                    NodeInfo::id,
                                    b ->
                                        JndiClient.of(b.host(), brokerIdToJmxPort.apply(b.id())))));
    var metricStore =
        MetricStore.builder()
            .beanExpiration(beanExpiration)
            .localReceiver(clientSupplier)
            .sensorsSupplier(
                () ->
                    sensors.metricSensors().stream()
                        .distinct()
                        .collect(
                            Collectors.toUnmodifiableMap(
                                Function.identity(), ignored -> (id, ee) -> {})))
            .build();
    server = Utils.packException(() -> HttpServer.create(new InetSocketAddress(port), 0));
    server.createContext("/topics", to(new TopicHandler(admin)));
    server.createContext("/groups", to(new GroupHandler(admin)));
    server.createContext("/brokers", to(new BrokerHandler(admin)));
    server.createContext("/producers", to(new ProducerHandler(admin)));
    server.createContext("/quotas", to(new QuotaHandler(admin)));
    server.createContext("/transactions", to(new TransactionHandler(admin)));
    server.createContext("/beans", to(new BeanHandler(admin, brokerIdToJmxPort)));
    server.createContext("/sensors", to(new SensorHandler(sensors)));
    server.createContext("/records", to(new RecordHandler(admin)));
    server.createContext("/reassignments", to(new ReassignmentHandler(admin)));
    server.createContext("/balancer", to(new BalancerHandler(admin, metricStore)));
    server.createContext("/throttles", to(new ThrottleHandler(admin)));
    server.start();
  }

  public int port() {
    return server.getAddress().getPort();
  }

  @Override
  public void close() {
    Utils.close(admin);
    server.stop(3);
  }

  public static void main(String[] args) throws Exception {
    var arg = org.astraea.app.argument.Argument.parse(new Argument(), args);
    if (arg.jmxPort < 0 && arg.jmxPorts.isEmpty())
      throw new IllegalArgumentException("you must define either --jmx.port or --jmx.ports");
    try (var service =
        new WebService(
            Admin.of(arg.configs()), arg.port, arg::jmxPortMapping, arg.beanExpiration)) {
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

  static class Argument extends org.astraea.app.argument.Argument {
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
        validateWith = IntegerMapField.class,
        converter = IntegerMapField.class)
    Map<Integer, Integer> jmxPorts = Map.of();

    int jmxPortMapping(int brokerId) {
      int port = jmxPorts.getOrDefault(brokerId, jmxPort);
      if (port < 0)
        throw new IllegalArgumentException("Failed to get jmx port for broker: " + brokerId);
      return port;
    }

    @Parameter(
        names = {"--ttl"},
        description = "Duration: the life of web service",
        validateWith = DurationField.class,
        converter = DurationField.class)
    Duration ttl = null;

    @Parameter(
        names = {"--bean.expiration"},
        description = "Duration: the life of collected metrics",
        validateWith = DurationField.class,
        converter = DurationField.class)
    Duration beanExpiration = Duration.ofHours(1);
  }

  static class Sensors {
    private final Collection<MetricSensor> sensors;

    Sensors() {
      sensors = new ConcurrentLinkedQueue<>();
    }

    Collection<MetricSensor> metricSensors() {
      return sensors;
    }

    void clearSensors() {
      sensors.clear();
    }

    void addSensors(MetricSensor metricSensor) {
      sensors.add(metricSensor);
    }
  }
}
