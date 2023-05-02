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
  // TODO: Add tests for the above requirement described in javadoc.

  private BalancerConfigs() {}

  /**
   * A regular expression indicates which topics are eligible for rebalancing. When specified,
   * topics that don't match this expression cannot be altered and must stay at their original
   * position.
   */
  public static final String BALANCER_ALLOWED_TOPICS_REGEX = "balancer.allowed.topics.regex";

  /**
   * A regular expression indicates which brokers are eligible for moving loading. When specified, a
   * broker with an id that doesn't match this expression cannot accept a partition from the other
   * broker or move its partition to other brokers.
   */
  public static final String BALANCER_ALLOWED_BROKERS_REGEX = "balancer.allowed.brokers.regex";
}
