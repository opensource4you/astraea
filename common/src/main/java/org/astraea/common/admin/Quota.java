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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.common.quota.ClientQuotaEntity;

public class Quota {

  static List<Quota> of(Map<ClientQuotaEntity, Map<String, Double>> data) {
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
                                            stringStringEntry.getKey(),
                                            stringStringEntry.getValue(),
                                            v.getKey(),
                                            v.getValue()))))
        .collect(Collectors.toUnmodifiableList());
  }

  private final String targetKey;

  private final String targetValue;
  private final String limitKey;
  private final double limitValue;

  public Quota(String targetKey, String targetValue, String limitKey, double limitValue) {
    this.targetKey = targetKey;
    this.targetValue = targetValue;
    this.limitKey = limitKey;
    this.limitValue = limitValue;
  }

  public String targetKey() {
    return targetKey;
  }

  public String targetValue() {
    return targetValue;
  }

  public String limitKey() {
    return limitKey;
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
        && Objects.equals(targetKey, quota.targetKey)
        && Objects.equals(targetValue, quota.targetValue)
        && Objects.equals(limitKey, quota.limitKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(targetKey, targetValue, limitKey, limitValue);
  }

  @Override
  public String toString() {
    return "Quota{"
        + "targetKey='"
        + targetKey
        + '\''
        + ", targetValue='"
        + targetValue
        + '\''
        + ", limitKey='"
        + limitKey
        + '\''
        + ", limitValue="
        + limitValue
        + '}';
  }
}
