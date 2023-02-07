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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.common.producer.Serializer;

public interface JMXPublisher {

  static List<JMXPublisher> create(
      int threads, String bootstrapServer, BlockingQueue<JMXFetcher.IdAndBean> idAndBeans) {
    var service = Executors.newFixedThreadPool(threads);
    var latches =
        IntStream.range(0, threads)
            .mapToObj(i -> new CountDownLatch(1))
            .collect(Collectors.toList());
    CompletableFuture.runAsync(
        () -> {
          try {
            latches.forEach(l -> Utils.swallowException(l::await));
          } finally {
            service.shutdown();
          }
        });
    return IntStream.range(0, threads)
        .mapToObj(
            i -> {
              var producer =
                  Producer.builder()
                      .bootstrapServers(bootstrapServer)
                      .valueSerializer(Serializer.STRING)
                      .build();
              var closed = new AtomicBoolean(false);
              var latch = latches.get(i);
              service.execute(
                  () -> {
                    while (!closed.get()) {
                      try {
                        var idAndBean = idAndBeans.poll(1, TimeUnit.SECONDS);
                        if (idAndBean == null) continue;
                        // Send mbean to internal topic.
                        Record<byte[], String> record =
                            Record.builder()
                                .topic(MetricPublisher.internalTopicName(idAndBean.id()))
                                .key((byte[]) null)
                                .value(idAndBean.bean().toString())
                                .build();
                        producer.send(record);
                      } catch (InterruptedException ie) {
                        ie.printStackTrace();
                      }
                    }
                    latch.countDown();
                  });
              return (JMXPublisher) () -> closed.set(true);
            })
        .collect(Collectors.toList());
  }

  void close();
}
