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
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.astraea.common.EnumInfo;

public class Quota {

  static Collection<Quota> of(Map<ClientQuotaEntity, Map<String, Double>> data) {
    return data.entrySet().stream()
        .flatMap(
            clientQuotaEntityMapEntry ->
                clientQuotaEntityMapEntry.getKey().entries().entrySet().stream()
                    .flatMap(
                        stringStringEntry ->
                            clientQuotaEntityMapEntry.getValue().entrySet().stream()
                                .map(
                                    v ->
                                        new Quota(
                                            target(stringStringEntry.getKey()),
                                            stringStringEntry.getValue(),
                                            limit(v.getKey()),
                                            v.getValue()))))
        .collect(Collectors.toUnmodifiableList());
  }

  static Target target(String value) {
    return Arrays.stream(Target.values())
        .filter(p -> p.nameOfKafka.equalsIgnoreCase(value))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("unknown: " + value));
  }

  static Limit limit(String value) {
    return Arrays.stream(Limit.values())
        .filter(p -> p.nameOfKafka.equalsIgnoreCase(value))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("unknown: " + value));
  }

  public enum Target implements EnumInfo {
    USER(ClientQuotaEntity.USER),
    CLIENT_ID(ClientQuotaEntity.CLIENT_ID),
    IP(ClientQuotaEntity.IP);

    public static Target ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(Target.class, alias);
    }

    private final String nameOfKafka;

    Target(String nameOfKafka) {
      this.nameOfKafka = nameOfKafka;
    }

    public String nameOfKafka() {
      return nameOfKafka;
    }

    @Override
    public String alias() {
      return nameOfKafka;
    }

    @Override
    public String toString() {
      return alias();
    }
  }

  public enum Limit implements EnumInfo {
    PRODUCER_BYTE_RATE("producer_byte_rate"),
    CONSUMER_BYTE_RATE("consumer_byte_rate"),
    REQUEST_PERCENTAGE("request_percentage"),
    CONTROLLER_MUTATION_RATE("controller_mutation_rate"),
    IP_CONNECTION_RATE("connection_creation_rate");

    public static Limit ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(Limit.class, alias);
    }

    private final String nameOfKafka;

    Limit(String nameOfKafka) {
      this.nameOfKafka = nameOfKafka;
    }

    public String nameOfKafka() {
      return nameOfKafka;
    }

    @Override
    public String alias() {
      return nameOfKafka();
    }

    @Override
    public String toString() {
      return alias();
    }
  }

  private final Target target;

  private final String targetValue;
  private final Limit limit;
  private final double limitValue;

  public Quota(Target target, String targetValue, Limit limit, double limitValue) {
    this.target = target;
    this.targetValue = targetValue;
    this.limit = limit;
    this.limitValue = limitValue;
  }

  public Target target() {
    return target;
  }

  public String targetValue() {
    return targetValue;
  }

  public Limit limit() {
    return limit;
  }

  public double limitValue() {
    return limitValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Quota quota = (Quota) o;
    return Double.compare(quota.limitValue, limitValue) == 0
        && target == quota.target
        && Objects.equals(targetValue, quota.targetValue)
        && limit == quota.limit;
  }

  @Override
  public int hashCode() {
    return Objects.hash(target, targetValue, limit, limitValue);
  }

  @Override
  public String toString() {
    return "Quota{"
        + "target="
        + target
        + ", targetValue='"
        + targetValue
        + '\''
        + ", limit="
        + limit
        + ", limitValue="
        + limitValue
        + '}';
  }
}
