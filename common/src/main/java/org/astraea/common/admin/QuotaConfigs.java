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

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.EnumInfo;

public final class QuotaConfigs {

  public enum QuotaKeys implements EnumInfo {
    // ---------------------------------[target key]---------------------------------//
    USER("user"),
    CLIENT_ID("clientId", "client-id"),
    IP("ip"),

    // ---------------------------------[limit key]---------------------------------//
    PRODUCER_BYTE_RATE("producerByteRate", "producer_byte_rate"),
    CONSUMER_BYTE_RATE("consumerByteRate", "consumer_byte_rate"),
    IP_CONNECTION_RATE("connectionCreationRate", "connection_creation_rate");

    private static final Map<String, QuotaKeys> quotaKeysMap =
        Arrays.stream(QuotaKeys.values())
            .collect(Collectors.toMap(x -> x.value, Function.identity()));

    private static final Map<String, QuotaKeys> quotaKafkaKeysMap =
        Arrays.stream(QuotaKeys.values())
            .collect(Collectors.toMap(x -> x.kafkaValue, Function.identity()));

    public static QuotaKeys ofAlias(String value) {
      return quotaKeysMap.get(value);
    }

    public static QuotaKeys fromKafka(String value) {
      return quotaKafkaKeysMap.get(value);
    }

    private final String value;
    private final String kafkaValue;

    QuotaKeys(String value) {
      this.value = value;
      this.kafkaValue = value;
    }

    QuotaKeys(String value, String kafkaValue) {
      this.value = value;
      this.kafkaValue = kafkaValue;
    }

    public String value() {
      return value;
    }

    public String kafkaValue() {
      return kafkaValue;
    }

    @Override
    public String alias() {
      return value;
    }

    @Override
    public String toString() {
      return alias();
    }
  }

  private QuotaConfigs() {}
}
