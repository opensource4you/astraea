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
package org.astraea.app.ImportCsv;

import com.beust.jcommander.ParameterException;
import java.io.File;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FileFromTest {
  private final FileFrom.Field field = new FileFrom.Field();

  @Test
  void testUnknownArgument() {
    Assertions.assertThrows(ParameterException.class, () -> field.validate("--source.file", "aaa"));
    Assertions.assertThrows(ParameterException.class, () -> field.convert("local://"));
    Assertions.assertThrows(ParameterException.class, () -> field.convert("local:/"));
  }

  @Test
  void testLocal() {
    Assertions.assertDoesNotThrow(() -> field.validate("--source", "local:///tmp"));
    var fileFrom = field.convert("local:///tmp");
    Assertions.assertEquals("/tmp", fileFrom.toString());
    fileFrom.fileSystem().mkdir("/testMkdir");
    Assertions.assertTrue(new File("/tmp/testMkdir").exists());
  }
}
