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
package org.astraea.common.cost;

/** Return type of cost function, `HasMoveCost`. It returns the score of brokers. */
public interface ClusterCost {

  /**
   * The cost score of a Kafka cluster. This value represents the idealness of a Kafka cluster in
   * terms of a specific performance aspect.
   *
   * <p>This value should be bounded between the range of [0, 1]. The value of 0 represents the
   * Kafka cluster is in the best condition regarding the specific performance aspect. And 1
   * represents the opposite(worst condition). Given two cost numbers `a` and `b` from two different
   * cluster states `A` and `B`, if a < b then we can state that cluster state A is better than
   * cluster state B in terms of the specific performance aspect.
   *
   * @return a number represents the idealness of a cluster state in terms of specific performance
   *     aspect.
   */
  double value();
}
