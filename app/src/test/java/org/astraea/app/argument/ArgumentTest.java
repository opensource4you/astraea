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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ArgumentTest {

  @Test
  void testFilterEmpty() {
    var args0 = Argument.filterEmpty(new String[] {"", " ", "   "});
    Assertions.assertEquals(0, args0.length);

    var args1 = Argument.filterEmpty(new String[] {"", "1 ", "   "});
    Assertions.assertEquals(1, args1.length);
    Assertions.assertEquals("1", args1[0]);

    var args2 = Argument.filterEmpty(new String[] {"", " 1", " 2  "});
    Assertions.assertEquals(2, args2.length);
    Assertions.assertEquals("1", args2[0]);
    Assertions.assertEquals("2", args2[1]);
  }

  @Test
  void testCommonProperties() throws IOException {
    var file = Files.createTempFile("test_basic_argument", "");
    try (var output = new BufferedWriter(new FileWriter(file.toFile()))) {
      output.write("key1=value1");
      output.newLine();
      output.write("key2=value2");
    }
    var argument =
        Argument.parse(
            new DumbArgument(),
            new String[] {
              "--bootstrap.servers", "abc", "--prop.file", file.toString(), "--configs", "a=b"
            });
    Assertions.assertEquals(1, argument.configs.size());
    Assertions.assertEquals("b", argument.configs.get("a"));
    Assertions.assertEquals(file.toString(), argument.propFile);
    // 2 (from prop file) + 1 (from configs) + 1 (bootstrap servers)
    Assertions.assertEquals(4, argument.configs().size());
    Assertions.assertEquals("abc", argument.bootstrapServers);
    Assertions.assertEquals("abc", argument.bootstrapServers());
    Assertions.assertEquals("value1", argument.configs().get("key1"));
    Assertions.assertEquals("value2", argument.configs().get("key2"));
  }

  private static class DumbArgument extends Argument {}
}
