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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.util.stream.Stream;
import org.astraea.app.admin.Compression;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CompressionFieldTest {

  private static class FakeParameter {
    @Parameter(
        names = {"--field"},
        converter = CompressionField.class,
        validateWith = CompressionField.class)
    public Compression value;
  }

  @Test
  void testConversion() {
    var arg = new CompressionField();
    Stream.of(Compression.values())
        .forEach(type -> Assertions.assertEquals(type, arg.convert(type.name())));
    Assertions.assertThrows(ParameterException.class, () -> arg.convert("aaa"));
  }

  @Test
  void testParse() {
    var arg = Argument.parse(new FakeParameter(), new String[] {"--field", "gzip"});
    Assertions.assertEquals(Compression.GZIP, arg.value);
  }
}
