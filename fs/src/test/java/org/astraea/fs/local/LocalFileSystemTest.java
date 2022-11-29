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
package org.astraea.fs.local;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;
import org.astraea.common.Configuration;
import org.astraea.fs.FileSystem;
import org.astraea.fs.FileSystemTest;

public class LocalFileSystemTest extends FileSystemTest {

  @Override
  protected FileSystem fileSystem() {
    try {
      var tmp = Files.createTempDirectory("test_local_fs");
      return FileSystem.local(Configuration.of(Map.of(LocalFileSystem.ROOT_KEY, tmp.toString())));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
