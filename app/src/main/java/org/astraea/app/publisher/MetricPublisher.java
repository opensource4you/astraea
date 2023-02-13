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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.app.argument.DurationField;
import org.astraea.app.argument.StringMapField;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;

/** Keep fetching all kinds of metrics and publish to inner topics. */
public class MetricPublisher {
  public static String internalTopicName(String id) {
    return "__" + id + "_broker_metrics";
  }

  public static void main(String[] args) {
    var arguments = Arguments.parse(new MetricPublisher.Arguments(), args);
    execute(arguments);
  }

  private static void execute(Arguments arguments) {
    // queue of ID of the target to fetch
    var targetIDQueue = new ArrayBlockingQueue<String>(2000);
    // queue of fetched beans
    var beanQueue = new ArrayBlockingQueue<IdBean>(2000);
    var idMBeanClient = new ConcurrentHashMap<String, MBeanClient>();
    var JMXFetcherThreads =
        IntStream.range(0, 3)
            .mapToObj(
                i ->
                    new RepeatedThread(
                        () -> {
                          try {
                            var targetID = targetIDQueue.poll(5, TimeUnit.SECONDS);
                            // Nothing to fetch
                            if (targetID == null) return;
                            var targetClient = idMBeanClient.getOrDefault(targetID, null);
                            // targetClient not exist currently
                            if (targetClient == null) return;
                            beanQueue.addAll(
                                targetClient.beans(BeanQuery.all()).stream()
                                    .map(bean -> new IdBean(targetID, bean))
                                    .collect(Collectors.toList()));
                          } catch (InterruptedException ie) {
                            // Queue polling was interrupted, print and ignore.
                            ie.printStackTrace();
                          }
                        }));
    var publisherThreads =
        IntStream.range(0, 2)
            .mapToObj(i -> JMXPublisher.create(arguments.bootstrapServers()))
            .map(
                publisher ->
                    new RepeatedThread(
                        () ->
                            Utils.swallowException(
                                () -> {
                                  var idAndBean = beanQueue.take();
                                  publisher.publish(idAndBean.id(), idAndBean.bean());
                                })))
            .collect(Collectors.toList());
    var periodicJobPool = Executors.newScheduledThreadPool(2);
    var threadPool = Executors.newFixedThreadPool(5);
    var admin = Admin.of(arguments.bootstrapServers());

    // Periodically submit "fetch job" on all targets
    var periodicFetch =
        periodicJobPool.scheduleAtFixedRate(
            // Block until target put into queue. To ensure all targets can be put into queue
            () ->
                idMBeanClient.forEach(
                    (id, bean) -> Utils.swallowException(() -> targetIDQueue.put(id))),
            0,
            arguments.period.toMillis(),
            TimeUnit.MILLISECONDS);

    // Periodically update MBeanClient. (MBeanClients may change when broker added into cluster)
    var periodicUpdate =
        periodicJobPool.scheduleAtFixedRate(
            () ->
                admin
                    .nodeInfos()
                    .thenAccept(
                        nodes ->
                            nodes.forEach(
                                node ->
                                    idMBeanClient.putIfAbsent(
                                        String.valueOf(node.id()),
                                        MBeanClient.jndi(
                                            node.host(),
                                            arguments.idToJmxPort().apply(node.id()))))),
            0,
            5,
            TimeUnit.MINUTES);

    // All JMXFetchers keep fetching on targets
    JMXFetcherThreads.forEach(threadPool::execute);
    // All publishers keep publish mbeans
    publisherThreads.forEach(threadPool::execute);

    // Run forever
    try {
      Thread.sleep(Long.MAX_VALUE);
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    } finally {
      idMBeanClient.forEach((id, f) -> f.close());
      publisherThreads.forEach(RepeatedThread::close);
      periodicFetch.cancel(false);
      periodicUpdate.cancel(false);
      Utils.swallowException(() -> periodicJobPool.awaitTermination(1, TimeUnit.MINUTES));
      Utils.swallowException(() -> threadPool.awaitTermination(1, TimeUnit.MINUTES));
      admin.close();
    }
  }

  private static class IdBean {
    private final String id;
    private final BeanObject bean;

    public IdBean(String id, BeanObject bean) {
      this.id = id;
      this.bean = bean;
    }

    public String id() {
      return this.id;
    }

    public BeanObject bean() {
      return bean;
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
