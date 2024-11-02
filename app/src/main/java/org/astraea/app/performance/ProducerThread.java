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
package org.astraea.app.performance;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.metrics.MBeanRegister;
import org.astraea.common.metrics.Sensor;
import org.astraea.common.metrics.stats.Avg;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;

interface ProducerThread extends AbstractThread {

  String DOMAIN_NAME = "org.astraea";
  String TYPE_PROPERTY = "type";

  String TYPE_VALUE = "producer";

  String AVG_PROPERTY = "avg";
  String ID_PROPERTY = "client-id";

  static List<ProducerThread> create(
      BlockingQueue<List<Record<byte[], byte[]>>> dataQueue,
      Function<String, Producer<byte[], byte[]>> producerSupplier,
      int producers,
      int threads) {
    var producerAndSensors =
        IntStream.range(0, producers)
            .mapToObj(
                index -> {
                  var sensor = Sensor.builder().addStat(AVG_PROPERTY, Avg.of()).build();
                  var producer =
                      producerSupplier.apply(Performance.CLIENT_ID_PREFIX + "-producer-" + index);
                  // export the custom jmx for report thread
                  MBeanRegister.local()
                      .domainName(DOMAIN_NAME)
                      .property(TYPE_PROPERTY, TYPE_VALUE)
                      .property(ID_PROPERTY, producer.clientId())
                      .attribute(AVG_PROPERTY, Double.class, () -> sensor.measure(AVG_PROPERTY))
                      .register();
                  return Map.entry(producer, sensor);
                })
            .toList();
    List<ProducerThread> reports =
        IntStream.range(0, threads)
            .mapToObj(
                __ -> {
                  var closed = new AtomicBoolean(false);
                  var future =
                      CompletableFuture.runAsync(
                          () -> {
                            try {
                              while (!closed.get()) {
                                var index =
                                    ThreadLocalRandom.current().nextInt(producerAndSensors.size());
                                var producerAndSensor = producerAndSensors.get(index);
                                var data = dataQueue.poll(3, TimeUnit.SECONDS);
                                if (data == null) continue;
                                var now = System.currentTimeMillis();
                                producerAndSensor
                                    .getKey()
                                    .send(data)
                                    .forEach(
                                        f ->
                                            f.whenComplete(
                                                (r, e) -> {
                                                  if (e == null)
                                                    producerAndSensor
                                                        .getValue()
                                                        .record(
                                                            (double)
                                                                (System.currentTimeMillis() - now));
                                                }));
                              }
                            } catch (InterruptedException e) {
                              throw new RuntimeException(
                                  "The producer thread was prematurely closed.", e);
                            } finally {
                              closed.set(true);
                            }
                          });
                  return new ProducerThread() {

                    @Override
                    public boolean closed() {
                      return future.isDone();
                    }

                    @Override
                    public void waitForDone() {
                      Utils.swallowException(future::join);
                    }

                    @Override
                    public void close() {
                      closed.set(true);
                      waitForDone();
                    }
                  };
                })
            .collect(Collectors.toUnmodifiableList());
    // monitor
    CompletableFuture.runAsync(
        () -> {
          try {
            reports.forEach(l -> Utils.swallowException(l::waitForDone));
          } finally {
            producerAndSensors.forEach(p -> Utils.swallowException(() -> p.getKey().close()));
          }
        });
    return reports;
  }
}
