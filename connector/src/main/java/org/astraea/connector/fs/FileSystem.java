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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface FileSystem extends AutoCloseable {

  static FileSystem ftp(String hostname, int port, String user, String password) {
    return new FtpFileSystem(hostname, port, user, password);
  }

  void mkdir(String path);

  List<String> listFiles(String path);

  List<String> listFolders(String path);

  void delete(String path);

  InputStream read(String path);

  OutputStream write(String path);

  @Override
  void close();
}
