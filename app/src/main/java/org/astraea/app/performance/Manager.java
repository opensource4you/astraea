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
import java.util.function.Supplier;
import org.astraea.app.common.DataUnit;

/**
 * Thread safe This class is used for managing the start/end of the producer/consumer threads.
 * Producer can start producing until all consumers get assignment. Consumers can stop after all
 * producers are closed and all records are consumed.
 */
public class Manager {
  private final ExeTime exeTime;
  private final List<Metrics> producerMetrics, consumerMetrics;
  private final Supplier<Long> keyDistribution;

  /**
   * Used to manage producing/consuming.
   *
   * @param argument
   *     <ol>
   *       <li>"fixedSize" for setting whether the size of the record is fixed
   *       <li>"recordSize" for setting (the bound of) the size of the record
   *       <li>"consumers" for number of consumers to wait on getting assignment
   *       <li>"producers" for number of producers to wait on stop producing
   *       <li>"exeTime" for determining whether the producers/consumers are completed
   *     </ol>
   *
   * @param producerMetrics for counting the number of records have been produced
   * @param consumerMetrics for counting the number of records have been consumed
   */
  public Manager(
      Performance.Argument argument, List<Metrics> producerMetrics, List<Metrics> consumerMetrics) {
    if (argument.recordSize.greaterThan(DataUnit.Byte.of(Integer.MAX_VALUE)))
      throw new IllegalArgumentException(
          "Record size should be smaller than or equal to 2147483648 (Integer.MAX_VALUE) bytes");
    this.producerMetrics = producerMetrics;
    this.consumerMetrics = consumerMetrics;
    this.exeTime = argument.exeTime;
    this.keyDistribution = argument.keyDistributionType.create(100000);
  }

  public long producedRecords() {
    return producerMetrics.stream().mapToLong(Metrics::num).sum();
  }

  public long consumedRecords() {
    return consumerMetrics.stream().mapToLong(Metrics::num).sum();
  }

  public ExeTime exeTime() {
    return exeTime;
  }

  /** Check if we should keep consuming record. */
  public boolean consumedDone() {
    return consumerMetrics.size() == 0 || consumedRecords() >= producedRecords();
  }

  /** Randomly choose a key according to the distribution. */
  public byte[] getKey() {
    return (String.valueOf(keyDistribution.get())).getBytes();
  }
}
