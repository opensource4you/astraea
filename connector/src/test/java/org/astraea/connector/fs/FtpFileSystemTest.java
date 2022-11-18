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
package org.astraea.connector.fs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.astraea.it.FtpServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FtpFileSystemTest {

  @Test
  void testList() throws IOException {
    try (var server = FtpServer.local();
        var fs =
            FileSystem.ftp(server.hostname(), server.port(), server.user(), server.password())) {
      Assertions.assertEquals(0, fs.listFiles("/").size());
      Assertions.assertEquals(0, fs.listFolders("/").size());

      Assertions.assertThrows(IllegalArgumentException.class, () -> fs.listFiles("/aa"));
      Assertions.assertThrows(IllegalArgumentException.class, () -> fs.listFolders("/aa"));

      // create a file
      try (var output = fs.write("/aa")) {
        output.write("abc".getBytes(StandardCharsets.UTF_8));
      }
      var f = fs.listFiles("/");
      Assertions.assertEquals(1, f.size());
      Assertions.assertEquals("/aa", f.get(0));

      // can't list a file
      Assertions.assertThrows(IllegalArgumentException.class, () -> fs.listFiles("/aa"));
      Assertions.assertThrows(IllegalArgumentException.class, () -> fs.listFolders("/aa"));

      fs.mkdir("/bb");
      Assertions.assertEquals(0, fs.listFiles("/bb").size());
      Assertions.assertEquals(0, fs.listFolders("/bb").size());
    }
  }

  @Test
  void testMkdir() {
    try (var server = FtpServer.local();
        var fs =
            FileSystem.ftp(server.hostname(), server.port(), server.user(), server.password())) {
      Assertions.assertEquals(0, fs.listFolders("/").size());
      fs.mkdir("/tmp/aa");

      Assertions.assertEquals(1, fs.listFolders("/").size());
      Assertions.assertEquals(1, fs.listFolders("/tmp").size());
    }
  }

  @Test
  void testReadWrite() throws IOException {
    try (var server = FtpServer.local();
        var fs =
            FileSystem.ftp(server.hostname(), server.port(), server.user(), server.password())) {
      var path = "/aaa";
      try (var output = fs.write(path)) {
        output.write("abc".getBytes(StandardCharsets.UTF_8));
      }
      try (var input = fs.read(path)) {
        Assertions.assertEquals("abc", new String(input.readAllBytes(), StandardCharsets.UTF_8));
      }
    }
  }

  @Test
  void testWriteToCreateFolder() throws IOException {
    try (var server = FtpServer.local();
        var fs =
            FileSystem.ftp(server.hostname(), server.port(), server.user(), server.password())) {
      var path = "/tmp/aaa";
      try (var output = fs.write(path)) {
        output.write("abc".getBytes(StandardCharsets.UTF_8));
      }
      Assertions.assertEquals(1, fs.listFiles("/tmp").size());
      Assertions.assertEquals(1, fs.listFolders("/").size());
    }
  }

  @Test
  void testDelete() throws IOException {
    try (var server = FtpServer.local();
        var fs =
            FileSystem.ftp(server.hostname(), server.port(), server.user(), server.password())) {
      var path = "/tmp/aaa/bbb";
      try (var output = fs.write(path)) {
        output.write("abc".getBytes(StandardCharsets.UTF_8));
      }
      fs.delete("/tmp");
      Assertions.assertEquals(0, fs.listFolders("/").size());
    }
  }

  @Test
  void testDeleteRoot() {
    try (var server = FtpServer.local();
        var fs =
            FileSystem.ftp(server.hostname(), server.port(), server.user(), server.password())) {
      Assertions.assertThrows(IllegalArgumentException.class, () -> fs.delete("/"));
    }
  }
}
