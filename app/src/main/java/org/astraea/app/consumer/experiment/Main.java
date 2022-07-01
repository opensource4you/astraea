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
package org.astraea.app.consumer.experiment;

import com.beust.jcommander.Parameter;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.astraea.app.admin.Admin;

public class Main {
  static Map<Integer, ConcurrentLinkedQueue<RebalanceTime>> generationIDTime =
      new ConcurrentSkipListMap<>();

  public static void main(String[] args) throws InterruptedException {
    Argument argument = org.astraea.app.argument.Argument.parse(new Argument(), args);
    ArrayList<Consumer> consumers = createConsumers(argument);
    Trigger trigger = new Trigger();

    consumers.forEach(consumer -> consumer.start());
    trigger.killConsumers(consumers);

    for (Consumer consumer : consumers) {
      consumer.join();
    }
    printAvgTime();
  }

  private static Set<String> queryTopics(String bootstrapServer) {
    Admin admin = Admin.of(bootstrapServer);
    Set<String> topics = admin.topicNames();
    topics.remove("__consumer_offsets");
    admin.close();
    return topics;
  }

  private static ArrayList<Consumer> createConsumers(Argument argument) {
    ArrayList<Consumer> consumers = new ArrayList<>();
    Set<String> topics = queryTopics(argument.bootstrapServers());

    IntStream.range(0, argument.consumers)
        .boxed()
        .forEach(
            i -> {
              consumers.add(
                  new Consumer(
                      i,
                      new KafkaConsumer<>(
                          Map.of(
                              ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                              argument.bootstrapServers(),
                              ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                              argument.keyDeserializer,
                              ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                              argument.valueDeserializer,
                              ConsumerConfig.GROUP_ID_CONFIG,
                              argument.groupId,
                              ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                              argument.strategy)),
                      generationIDTime));
              consumers.get(i).doSubscribe(topics);
            });
    return consumers;
  }

  static class Argument extends org.astraea.app.argument.Argument {
    @Parameter(names = "--keyDeserializer")
    String keyDeserializer = StringDeserializer.class.getName();

    @Parameter(names = "--valueDeserializer")
    String valueDeserializer = StringDeserializer.class.getName();

    @Parameter(names = "--groupId", required = true)
    String groupId;

    @Parameter(names = "--strategy")
    String strategy = RangeAssignor.class.getName();

    @Parameter(names = "--consumers")
    int consumers = 1;
  }

  static void printAvgTime() {
    generationIDTime.forEach(
        (generationId, rebalanceTimes) -> {
          System.out.println("generationId #" + generationId);
          final int size = rebalanceTimes.size();

          double avgTime =
              (double)
                      rebalanceTimes.stream()
                          .mapToLong((rebalanceTime -> rebalanceTime.rebalanceTime().toMillis()))
                          .sum()
                  / size;
          System.out.printf("Average time : %.2fms\n", avgTime);
        });
  }
}
