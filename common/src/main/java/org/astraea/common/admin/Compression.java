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

import com.beust.jcommander.ParameterException;
import java.util.Locale;
import org.astraea.app.common.EnumInfo;

public enum Compression implements EnumInfo {
  NONE("none"),
  GZIP("gzip"),
  SNAPPY("snappy"),
  LZ4("lz4"),
  ZSTD("zstd");

  private final String alias;

  Compression(String alias) {
    this.alias = alias;
  }

  public static Compression ofAlias(String alias) {
    return EnumInfo.ignoreCaseEnum(Compression.class, alias);
  }

  public String alias() {
    return alias;
  }

  @Override
  public String toString() {
    return EnumInfo.alias2String(this);
  }

  /** @return the name parsed by kafka */
  public String nameOfKafka() {
    return name().toLowerCase(Locale.ROOT);
  }

  public static class Field extends org.astraea.common.argument.Field<Compression> {
    @Override
    public Compression convert(String value) {
      try {
        // `CompressionType#forName` accept lower-case name only.
        return Compression.ofAlias(value);
      } catch (IllegalArgumentException e) {
        throw new ParameterException(e);
      }
    }
  }
}
