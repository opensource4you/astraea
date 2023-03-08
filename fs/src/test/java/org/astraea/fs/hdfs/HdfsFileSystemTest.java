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
package org.astraea.fs.hdfs;

import java.util.Map;
import org.astraea.common.Configuration;
import org.astraea.fs.AbstractFileSystemTest;
import org.astraea.fs.FileSystem;
import org.astraea.it.HdfsServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HdfsFileSystemTest extends AbstractFileSystemTest {

  private final HdfsServer server = HdfsServer.local();

  @Test
  void testCreate() {
    var fs =
        HdfsFileSystem.create(
            Configuration.of(
                Map.of(
                    HdfsFileSystem.HOSTNAME_KEY,
                    server.hostname(),
                    HdfsFileSystem.PORT_KEY,
                    String.valueOf(server.port()),
                    HdfsFileSystem.USER_KEY,
                    server.user())));

    Assertions.assertEquals(
        fs.getUri().toString(), "hdfs://" + server.hostname() + ":" + server.port());
  }

  @Override
  protected FileSystem fileSystem() {
    return FileSystem.of(
        "hdfs",
        Configuration.of(
            Map.of(
                HdfsFileSystem.HOSTNAME_KEY,
                server.hostname(),
                HdfsFileSystem.PORT_KEY,
                String.valueOf(server.port()),
                HdfsFileSystem.USER_KEY,
                server.user())));
  }

  @AfterEach
  void close() {
    server.close();
  }
}
