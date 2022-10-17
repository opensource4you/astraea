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
package org.astraea.common.scenario;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.astraea.common.Utils;
import org.astraea.common.admin.AsyncAdmin;

/** The subclass of this class should contain the logic to fulfill a scenario. */
public interface Scenario {

  static Builder build(double binomialProbability) {
    return new Builder(binomialProbability);
  }

  class Builder {
    private String topicName = Utils.randomString();
    private int numberOfPartitions = 10;
    private short numberOfReplicas = 1;
    private double binomialProbability = 0.5;

    private Builder(double binomialProbability) {
      this.binomialProbability = binomialProbability;
    }

    public Builder topicName(String topicName) {
      this.topicName = topicName;
      return this;
    }

    public Builder numberOfPartitions(int numberOfPartitions) {
      this.numberOfPartitions = numberOfPartitions;
      return this;
    }

    public Builder numberOfReplicas(short numberOfReplicas) {
      this.numberOfReplicas = numberOfReplicas;
      return this;
    }

    public Builder binomialProbability(double binomialProbability) {
      this.binomialProbability = binomialProbability;
      return this;
    }

    public Scenario build() {
      return new SkewedPartitionScenario(
          topicName, numberOfPartitions, numberOfReplicas, binomialProbability);
    }
  }

  /** Apply this scenario to the Kafka cluster */
  CompletionStage<Result> apply(AsyncAdmin admin);

  class Result {

    private final String topicName;
    private final int numberOfPartitions;
    private final short numberOfReplicas;
    private final Map<Integer, Long> leaderSum;
    private final Map<Integer, Long> logSum;

    public Result(
        String topicName,
        int numberOfPartitions,
        short numberOfReplicas,
        Map<Integer, Long> leaderSum,
        Map<Integer, Long> logSum) {
      this.topicName = topicName;
      this.numberOfPartitions = numberOfPartitions;
      this.numberOfReplicas = numberOfReplicas;
      this.leaderSum = leaderSum;
      this.logSum = logSum;
    }

    public String topicName() {
      return topicName;
    }

    public int numberOfPartitions() {
      return numberOfPartitions;
    }

    public short numberOfReplicas() {
      return numberOfReplicas;
    }

    public Map<Integer, Long> leaderSum() {
      return leaderSum;
    }

    public Map<Integer, Long> logSum() {
      return logSum;
    }
  }
}
