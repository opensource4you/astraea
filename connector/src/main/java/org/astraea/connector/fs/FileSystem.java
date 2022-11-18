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

  /**
   * create the folder if it is nonexistent. The parent folders get created automatically.
   *
   * @param path to make a folder
   */
  void mkdir(String path);

  /**
   * List all files of path. It throws exception if the path is not a folder.
   *
   * @param path to list file
   * @return file names
   */
  List<String> listFiles(String path);

  /**
   * List all folders of path. It throws exception if the path is not a folder.
   *
   * @param path to list file
   * @return folder names
   */
  List<String> listFolders(String path);

  /**
   * delete the file or whole folder
   *
   * @param path to delete
   */
  void delete(String path);

  InputStream read(String path);

  /**
   * create a file on given path. The parent folders get created automatically.
   *
   * @param path to write data
   * @return output stream
   */
  OutputStream write(String path);

  @Override
  void close();
}
