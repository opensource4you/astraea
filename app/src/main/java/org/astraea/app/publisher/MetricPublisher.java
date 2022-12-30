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
package org.astraea.app.publisher;

import com.beust.jcommander.Parameter;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.astraea.app.argument.DurationField;
import org.astraea.app.argument.StringMapField;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;

/** Keep fetching all kinds of metrics and publish to inner topics. */
public class MetricPublisher {
  public static String internalTopicName(int brokerId) {
    return "__" + brokerId + "_broker_metrics";
  }

  public static void main(String[] args) {
    var arguments = Arguments.parse(new MetricPublisher.Arguments(), args);
    execute(arguments);
  }

  private static void execute(Arguments arguments) {
    var clusterInfoUpdate =
        Utils.toDuration(
            arguments
                .configs()
                .getOrDefault(MetricPublisherConfig.CLUSTER_INFO_UPDATE_DURATION.alias(), "1m"));

    System.out.println(clusterInfoUpdate.toMillis());
    try (var admin = Admin.of(arguments.bootstrapServers())) {
      var nodeInfos = admin.nodeInfos().toCompletableFuture().get();

      System.out.println("Fetching node information from " + arguments.bootstrapServers() + " ...");
      var jmxPublisher =
          new JMXPublisher(arguments.bootstrapServers(), nodeInfos, arguments.idToJmxPort());
      var scheduler = Executors.newScheduledThreadPool(2);

      // Update node infos to all publisher
      scheduler.scheduleAtFixedRate(
          () -> {
            try {
              admin
                  .nodeInfos()
                  .thenAccept(jmxPublisher::updateNodeInfo)
                  .toCompletableFuture()
                  .get();
            } catch (InterruptedException | ExecutionException e) {
              // could not fetch node info, don't throw, keep running
              e.printStackTrace();
            }
          },
          clusterInfoUpdate.toMillis(),
          clusterInfoUpdate.toMillis(),
          TimeUnit.MILLISECONDS);

      // Put all publisher to scheduler
      scheduler.scheduleAtFixedRate(
          jmxPublisher,
          arguments.duration.toMillis(),
          arguments.duration.toMillis(),
          TimeUnit.MILLISECONDS);

      System.out.println("Publisher started.");
      scheduler.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    } catch (ExecutionException | InterruptedException e) {
      // node info fetching exception, don't throw, keep running
      e.printStackTrace();
    }
  }

  public static class Arguments extends org.astraea.app.argument.Argument {
    @Parameter(
        names = {"--jmxAddress"},
        description =
            "<brokerId>=<jmxAddress>: Pairs of broker id and its corresponding jmx address",
        converter = StringMapField.class,
        validateWith = StringMapField.class)
    public Map<String, String> jmxAddress = Map.of();

    @Parameter(
        names = {"--jmxPort"},
        description =
            "String: The default port of jmx server of the brokers. For those brokers that"
                + " jmx server addresses are not set in \"--jmxAddress\", this port will be used"
                + " to connect that broker's jmx server.")
    public String defaultPort = null;

    @Parameter(
        names = {"--duration"},
        description = "Duration: The rate to fetch and publish metrics. Default: 10s",
        validateWith = DurationField.class,
        converter = DurationField.class)
    public Duration duration = Duration.ofSeconds(10);

    public Function<Integer, Integer> idToJmxPort() {
      return id -> Integer.parseInt(jmxAddress.getOrDefault(id.toString(), defaultPort));
    }
  }
}
