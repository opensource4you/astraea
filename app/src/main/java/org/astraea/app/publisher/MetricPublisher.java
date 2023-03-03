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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
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

  // Valid for testing
  static void execute(Arguments arguments) {
    // self-defined queue of ID and target mbean clients to fetch
    var targetClients = new DelayQueue<DelayedIdClient>();
    // queue of fetched beans
    var beanQueue = new ArrayBlockingQueue<IdBean>(2000);
    var close = new AtomicBoolean(false);

    var JMXFetcherThreads =
        jmxFetcherThreads(3, targetClients, arguments.period, beanQueue, close::get);
    var publisherThreads = publisherThreads(2, arguments.bootstrapServers(), beanQueue, close::get);
    var periodicJobPool = Executors.newScheduledThreadPool(1);
    var threadPool = Executors.newFixedThreadPool(3 + 2);
    var admin = Admin.of(arguments.bootstrapServers());

    // Periodically update MBeanClient. (MBeanClients may change when broker added into cluster)
    var periodicUpdate =
        periodicJobPool.scheduleAtFixedRate(
            () ->
                admin
                    .nodeInfos()
                    .thenAccept(
                        nodes -> {
                          // TODO: Real elements in queue may not consistent with `inQueue` object.
                          // Other threads (JMXFetcherThreads) may add element to this queue (as
                          // they complete the fetch). This may result in duplicate targets in the
                          // DelayQueue (`targetClients`). Discussion:
                          // https://github.com/skiptests/astraea/pull/1481#discussion_r1112972154
                          var inQueue =
                              targetClients.stream()
                                  .map(target -> target.id)
                                  .collect(Collectors.toUnmodifiableSet());
                          nodes.stream()
                              .filter(node -> !inQueue.contains(String.valueOf(node.id())))
                              .forEach(
                                  node ->
                                      targetClients.put(
                                          new DelayedIdClient(
                                              arguments.period,
                                              String.valueOf(node.id()),
                                              MBeanClient.jndi(
                                                  node.host(),
                                                  arguments.idToJmxPort().apply(node.id())))));
                        }),
            0,
            5,
            TimeUnit.MINUTES);

    // All JMXFetchers keep fetching on targets
    JMXFetcherThreads.forEach(threadPool::execute);
    // All publishers keep publish mbeans
    publisherThreads.forEach(threadPool::execute);

    // Run until the given time-to-live passed
    try {
      Thread.sleep(arguments.ttl.toMillis());
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    } finally {
      close.set(true);
      periodicUpdate.cancel(false);
      periodicJobPool.shutdown();
      threadPool.shutdown();
      Utils.swallowException(() -> periodicJobPool.awaitTermination(1, TimeUnit.MINUTES));
      Utils.swallowException(() -> threadPool.awaitTermination(1, TimeUnit.MINUTES));
      targetClients.forEach(idClient -> idClient.mBeanClient.close());

      admin.close();
    }
  }

  private static List<Runnable> jmxFetcherThreads(
      int threads,
      DelayQueue<DelayedIdClient> clients,
      Duration duration,
      BlockingQueue<IdBean> beanQueue,
      Supplier<Boolean> closed) {
    return IntStream.range(0, threads)
        .mapToObj(
            i ->
                (Runnable)
                    () -> {
                      while (!closed.get()) {
                        try {
                          var delayedClient = clients.poll(5, TimeUnit.SECONDS);
                          if (delayedClient != null) {
                            beanQueue.addAll(
                                delayedClient.mBeanClient.beans(BeanQuery.all()).stream()
                                    .map(bean -> new IdBean(delayedClient.id, bean))
                                    .collect(Collectors.toList()));
                            clients.put(
                                new DelayedIdClient(
                                    duration, delayedClient.id, delayedClient.mBeanClient));
                          }
                        } catch (InterruptedException ie) {
                          // Interrupted while polling delay-queue, end this thread
                          ie.printStackTrace();
                          return;
                        }
                      }
                    })
        .collect(Collectors.toList());
  }

  private static List<Runnable> publisherThreads(
      int threads, String bootstrap, BlockingQueue<IdBean> beanQueue, Supplier<Boolean> closed) {
    return IntStream.range(0, threads)
        .mapToObj(
            i ->
                (Runnable)
                    () -> {
                      try (var publisher = JMXPublisher.create(bootstrap)) {
                        while (!closed.get()) {
                          try {
                            var idBean = beanQueue.poll(5, TimeUnit.SECONDS);
                            if (idBean != null) {
                              publisher.publish(idBean.id(), idBean.bean());
                            }
                          } catch (InterruptedException ie) {
                            // Interrupted while polling delay-queue, end this thread
                            ie.printStackTrace();
                            return;
                          }
                        }
                      }
                    })
        .collect(Collectors.toList());
  }

  static class DelayedIdClient implements Delayed {
    private final long timeout;

    private final String id;
    private final MBeanClient mBeanClient;

    public DelayedIdClient(Duration duration, String id, MBeanClient mBeanClient) {
      this.timeout = System.nanoTime() + duration.toNanos();
      this.id = id;
      this.mBeanClient = mBeanClient;
    }

    @Override
    public long getDelay(TimeUnit timeUnit) {
      return timeUnit.convert(timeout - System.nanoTime(), TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed delayed) {
      return Long.compare(
          this.getDelay(TimeUnit.NANOSECONDS), delayed.getDelay(TimeUnit.NANOSECONDS));
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

    @Parameter(
        names = {"--ttl"},
        description = "Duration: Time to live. Default: about 10^10 days.",
        validateWith = DurationField.class,
        converter = DurationField.class)
    public Duration ttl = Duration.ofMillis(Long.MAX_VALUE);

    public Function<Integer, Integer> idToJmxPort() {
      return id -> Integer.parseInt(jmxAddress.getOrDefault(id.toString(), defaultPort));
    }
  }
}
