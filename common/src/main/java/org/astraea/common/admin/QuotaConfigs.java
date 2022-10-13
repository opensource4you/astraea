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
package org.astraea.common.admin;

public final class QuotaConfigs {

  // ---------------------------------[target key]---------------------------------//
  public static final String USER = "user";
  public static final String CLIENT_ID = "client-id";
  public static final String IP = "ip";

  // ---------------------------------[limit key]---------------------------------//
  public static final String PRODUCER_BYTE_RATE_CONFIG = "producer_byte_rate";
  public static final String CONSUMER_BYTE_RATE_CONFIG = "consumer_byte_rate";
  public static final String REQUEST_PERCENTAGE_CONFIG = "request_percentage";
  public static final String CONTROLLER_MUTATION_RATE_CONFIG = "controller_mutation_rate";
  public static final String IP_CONNECTION_RATE_CONFIG = "connection_creation_rate";

  private QuotaConfigs() {}
}
