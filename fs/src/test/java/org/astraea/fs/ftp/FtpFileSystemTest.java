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
package org.astraea.fs.ftp;

import java.util.Map;
import org.astraea.common.Configuration;
import org.astraea.fs.FileSystem;
import org.astraea.fs.FileSystemTest;
import org.astraea.it.FtpServer;
import org.junit.jupiter.api.AfterEach;

public class FtpFileSystemTest extends FileSystemTest {

  private final FtpServer server = FtpServer.local();

  @Override
  protected FileSystem fileSystem() {
    return FileSystem.ftp(
        Configuration.of(
            Map.of(
                FtpFileSystem.HOSTNAME_KEY,
                server.hostname(),
                FtpFileSystem.PORT_KEY,
                String.valueOf(server.port()),
                FtpFileSystem.USER_KEY,
                server.user(),
                FtpFileSystem.PASSWORD_KEY,
                server.password())));
  }

  @AfterEach
  void close() {
    server.close();
  }
}
