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
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.astraea.common.Configuration;
import org.astraea.fs.ftp.FtpFileSystem;
import org.astraea.fs.local.LocalFileSystem;

public interface FileSystem extends AutoCloseable {
  static FileSystem of(URI uri) {
    var HOSTNAME_KEY = "fs.ftp.hostname";
    var PORT_KEY = "fs.ftp.port";
    var USER_KEY = "fs.ftp.user";
    var PASSWORD_KEY = "fs.ftp.password";

    if (uri.getScheme().equals("local")) {
      return FileSystem.local(Configuration.of(Map.of()));
    } else if (uri.getScheme().equals("ftp")) {
      String[] userInfo = uri.getUserInfo().split(":", 2);
      return FileSystem.ftp(
          Configuration.of(
              Map.of(
                  HOSTNAME_KEY,
                  uri.getHost(),
                  PORT_KEY,
                  String.valueOf(uri.getPort()),
                  USER_KEY,
                  userInfo[0],
                  PASSWORD_KEY,
                  userInfo[1])));
    } else {
      throw new IllegalArgumentException(uri.getScheme() + ", schema mismatch.");
    }
  }

  static FileSystem ftp(Configuration configuration) {
    return new FtpFileSystem(configuration);
  }

  static FileSystem local(Configuration configuration) {
    return new LocalFileSystem(configuration);
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

  /**
   * @param path to check type
   * @return the type of path
   */
  Type type(String path);

  @Override
  void close();

  // ---------------------[helper]---------------------//

  static String path(String root, String name) {
    if (root.endsWith("/")) return root + name;
    return root + "/" + name;
  }

  static String parent(String path) {
    if (path.equals("/")) return null;
    var index = path.lastIndexOf("/");
    if (index < 0) throw new IllegalArgumentException("illegal path: " + path);
    return path.substring(0, index);
  }
}
