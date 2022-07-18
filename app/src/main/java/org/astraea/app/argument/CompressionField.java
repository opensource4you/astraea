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
package org.astraea.app.argument;

import com.beust.jcommander.ParameterException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.record.CompressionType;
import org.astraea.app.admin.Compression;

public class CompressionField extends Field<Compression> {
  /**
   * @param value Name of compression type. Accept lower-case name only ("none", "gzip", "snappy",
   *     "lz4", "zstd").
   */
  @Override
  public Compression convert(String value) {
    try {
      // `CompressionType#forName` accept lower-case name only.
      return Compression.of(value);
    } catch (IllegalArgumentException e) {
      throw new ParameterException(
          "the "
              + value
              + " is unsupported. The supported algorithms are "
              + Stream.of(CompressionType.values())
                  .map(CompressionType::name)
                  .collect(Collectors.joining(",")));
    }
  }

  @Override
  protected void check(String name, String value) throws ParameterException {
    convert(value);
  }
}
