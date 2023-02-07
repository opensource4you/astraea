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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.astraea.app.argument.DurationField;
import org.astraea.app.argument.StringMapField;
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
    var idQueue = new ArrayBlockingQueue<Integer>(2000);
    var beanQueue = new ArrayBlockingQueue<JMXFetcher.IdAndBean>(2000);
    var jmxFetchers =
        JMXFetcher.create(
            3, arguments.bootstrapServers(), arguments.idToJmxPort(), idQueue, beanQueue);
    var jmxPublishers = JMXPublisher.create(2, arguments.bootstrapServers(), beanQueue);
    var executor = Executors.newScheduledThreadPool(1);

    try (var admin = Admin.of(arguments.bootstrapServers())) {
      // Periodically add targets to let the jmxFetchers fetch targets' mbean.
      executor.scheduleAtFixedRate(
          () ->
              admin
                  .nodeInfos()
                  .thenAccept(nodeInfos -> nodeInfos.forEach(node -> idQueue.offer(node.id()))),
          0,
          arguments.period.toMillis(),
          TimeUnit.MILLISECONDS);
      Thread.sleep(Long.MAX_VALUE);
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    } finally {
      jmxFetchers.forEach(JMXFetcher::close);
      jmxPublishers.forEach(JMXPublisher::close);
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
                + " to connect that broker's jmx server.",
        required = true)
    public String defaultPort = null;

    @Parameter(
        names = {"--period"},
        description = "Duration: The rate to fetch and publish metrics. Default: 10s",
        validateWith = DurationField.class,
        converter = DurationField.class)
    public Duration period = Duration.ofSeconds(10);

    public Function<Integer, Integer> idToJmxPort() {
      return id -> Integer.parseInt(jmxAddress.getOrDefault(id.toString(), defaultPort));
    }
  }
}
