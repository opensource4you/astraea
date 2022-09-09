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
package org.astraea.common.producer;

import com.beust.jcommander.ParameterException;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum Acks {
  /** wait for all isrs */
  ISRS("isrs"),
  /** wait for leader only */
  ONLY_LEADER("leader"),
  /** wait for nothing */
  NONE("none");

  private final String alias;

  Acks(String alias) {
    this.alias = alias;
  }

  public String alias() {
    return alias;
  }

  @Override
  public String toString() {
    return alias();
  }

  public static Acks ofAlias(String alias) {
    return Arrays.stream(Acks.values())
        .filter(a -> a.alias().equalsIgnoreCase(alias))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "the "
                        + alias
                        + " is unsupported. The supported algorithms are "
                        + Stream.of(Acks.values())
                            .map(Acks::alias)
                            .collect(Collectors.joining(","))));
  }

  public String valueOfKafka() {
    switch (this) {
      case ISRS:
        return "all";
      case ONLY_LEADER:
        return "1";
      default:
        return "0";
    }
  }

  public static class Field extends org.astraea.common.argument.Field<Acks> {

    @Override
    public Acks convert(String value) {
      try {
        return Acks.ofAlias(value);
      } catch (IllegalArgumentException e) {
        throw new ParameterException(e);
      }
    }
  }
}
