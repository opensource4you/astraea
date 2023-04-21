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

import java.util.concurrent.CompletionStage;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;

/** The subclass of this class should contain the logic to fulfill a scenario. */
public interface Scenario<Result> {

  static Builder builder() {
    return new Builder();
  }

  class Builder {
    private String topicName = Utils.randomString();
    private int numberOfPartitions = 10;
    private short numberOfReplicas = 1;
    private double binomialProbability = 0.5;

    private Builder() {}

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

    public Scenario<SkewedPartitionScenario.Result> build() {
      return new SkewedPartitionScenario(
          topicName, numberOfPartitions, numberOfReplicas, binomialProbability);
    }
  }

  /** Apply this scenario to the Kafka cluster */
  CompletionStage<Result> apply(Admin admin, Configuration scenarioConfig);
}
