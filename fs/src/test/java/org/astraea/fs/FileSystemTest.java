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
package org.astraea.fs;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.astraea.common.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FileSystemTest {

  @Test
  void testParent() {
    Assertions.assertEquals(Optional.empty(), FileSystem.parent("/"));
    Assertions.assertEquals("/", FileSystem.parent("/aaa").get());
    Assertions.assertEquals("/aaa", FileSystem.parent("/aaa/").get());
    Assertions.assertEquals(Optional.empty(), FileSystem.parent("aaa"));
  }

  @Test
  void testOf() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> FileSystem.of("unknown", Configuration.EMPTY));

    var fs = FileSystem.of("local", Configuration.of(Map.of("local.impl", Tmp.class.getName())));
    Assertions.assertInstanceOf(Tmp.class, fs);
  }

  private static class Tmp implements FileSystem {

    public Tmp(Configuration configuration) {}

    @Override
    public void mkdir(String path) {}

    @Override
    public List<String> listFiles(String path) {
      return null;
    }

    @Override
    public List<String> listFolders(String path) {
      return null;
    }

    @Override
    public void delete(String path) {}

    @Override
    public InputStream read(String path) {
      return null;
    }

    @Override
    public OutputStream write(String path) {
      return null;
    }

    @Override
    public Type type(String path) {
      return null;
    }

    @Override
    public void close() {}
  }
}
