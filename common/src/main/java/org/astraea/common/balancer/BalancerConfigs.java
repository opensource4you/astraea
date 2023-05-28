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
package org.astraea.common.balancer;

/**
 * A collection of official balancer capabilities. Noted that all these capabilities are optional,
 * the concrete balancer implementation might not support it. All the capability names must start
 * with a "balancer." prefix, and when the implementation sees an unsupported capability, it should
 * raise an exception.
 */
public final class BalancerConfigs {

  private BalancerConfigs() {}

  /**
   * A regular expression indicates which topics are eligible for balancing. When specified, topics
   * that don't match this expression cannot be altered and must stay at their original position.
   */
  public static final String BALANCER_ALLOWED_TOPICS_REGEX = "balancer.allowed.topics.regex";

  /**
   * This configuration indicates the balancing mode for each broker.
   *
   * <p>This configuration requires a string with a series of key-value pairs, each pair is
   * separated by a comma, and the key and value are separated by a colon. <code>
   *  (brokerId_A|"default"):(mode),(brokerId_B):(mode), ...</code> The key indicates the integer id
   * for a broker. And the value indicates the balancing mode for the associated broker. When the
   * key is a string value <code>"default"</code>(without the double quotes), it indicates the
   * associated balancing mode should be the default mode for the rest of the brokers that are not
   * addressed in the configuration. By default, all the brokers use <code> "balancing"</code> mode.
   *
   * <h3>Possible balancing modes</h3>
   *
   * <ul>
   *   <li><code>balancing</code>: The broker will participate in the load balancing process. The
   *       replica assignment for this broker is eligible for changes.
   *   <li><code>clear</code>: The broker should become empty after the rebalance. This mode allows
   *       the user to clear all the loadings for certain brokers, enabling a graceful removal of
   *       those brokers. Note to the balancer implementation: A broker in this mode assumes it will
   *       be out of service after the balancing is finished. Therefore, when evaluating the cluster
   *       cost, the brokers to clear should be excluded. However, these brokers will be included in
   *       the move cost evaluation. Since these brokers are still part of the cluster right now,
   *       and move cost focusing on the cost associated during the ongoing balancing process
   *       itself.
   *   <li><code>excluded</code>: The broker will not participate in the load balancing process. The
   *       replica assignment for this broker is not eligible for changes. It will neither accept
   *       replicas from other brokers nor reassign replicas to other brokers.
   * </ul>
   *
   * <h3>Flag Interaction:</h3>
   *
   * <ol>
   *   <li>All partitions on the clearing brokers will be compelled to participate in the balancing
   *       process, regardless of the explicit prohibition specified by the {@link
   *       BalancerConfigs#BALANCER_ALLOWED_TOPICS_REGEX} configuration. This exception solely
   *       applies to partitions located at a clearing broker, while disallowed partitions on
   *       balancing brokers will remain excluded from the balancing decision.
   * </ol>
   *
   * <h3>Limitation:</h3>
   *
   * <ol>
   *   <li>Clearing a broker may be infeasible if there are not enough brokers to fit the required
   *       replica factor for a specific partition. This situation is more likely to occur if there
   *       are many <code>excluded</code> brokers that reject accepting new replicas. If such a case
   *       is detected, an exception should be raised.
   *   <li>Any broker with ongoing replica-move-in, replica-move-out, or inter-folder movement
   *       cannot be the clearing target. An exception will be raised if any of the clearing brokers
   *       have such ongoing events.
   * </ol>
   */
  public static final String BALANCER_BROKER_BALANCING_MODE = "balancer.broker.balancing.mode";
}
