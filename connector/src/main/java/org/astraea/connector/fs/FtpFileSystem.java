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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.astraea.common.Utils;

class FtpFileSystem implements FileSystem {

  private enum Type {
    NONEXISTENT,
    FILE,
    FOLDER;
  }

  private static FTPClient create(String hostname, int port, String user, String password) {
    return Utils.packException(
        () -> {
          var client = new FTPClient();
          client.connect(hostname, port);
          client.enterLocalPassiveMode();
          // the data connection can be different from control connection
          client.setRemoteVerificationEnabled(false);
          if (!client.login(user, password))
            throw new IllegalArgumentException("failed to login ftp server");
          return client;
        });
  }

  private final FTPClient client;

  FtpFileSystem(String hostname, int port, String user, String password) {
    client = create(hostname, port, user, password);
  }

  private Type type(String path) {

    return Utils.packException(
        () -> {
          var stats = client.getStatus(path);
          if (stats == null) return Type.NONEXISTENT;
          var fs = client.listFiles(path);
          // RFC 959: If the pathname specifies a file then the server should send current
          // information on the file
          if (fs.length == 1 && path.endsWith(fs[0].getName()) && fs[0].isFile()) return Type.FILE;
          return Type.FOLDER;
        });
  }

  @Override
  public void mkdir(String path) {
    if (type(path) == Type.FOLDER) return;
    var parent = parent(path);
    if (parent != null && type(path) == Type.NONEXISTENT) mkdir(parent);
    Utils.packException(
        () -> {
          if (!client.changeWorkingDirectory(path) && !client.makeDirectory(path))
            throw new IllegalArgumentException("Failed to create folder on " + path);
        });
  }

  @Override
  public List<String> listFiles(String path) {
    if (type(path) != Type.FOLDER) throw new IllegalArgumentException(path + " is not a folder");
    return Utils.packException(
        () ->
            Arrays.stream(client.listFiles(path, FTPFile::isFile))
                .map(f -> path(path, f.getName()))
                .collect(Collectors.toList()));
  }

  @Override
  public List<String> listFolders(String path) {
    if (type(path) != Type.FOLDER) throw new IllegalArgumentException(path + " is not a folder");
    return Utils.packException(
        () ->
            Arrays.stream(client.listFiles(path, FTPFile::isDirectory))
                .map(f -> path(path, f.getName()))
                .collect(Collectors.toList()));
  }

  @Override
  public void delete(String path) {
    if (path.equals("/")) throw new IllegalArgumentException("Can't delete whole root folder");
    switch (type(path)) {
      case NONEXISTENT:
        return;
      case FILE:
        Utils.packException(() -> client.deleteFile(path));
        return;
      case FOLDER:
        Utils.packException(
            () -> {
              for (var f : client.listFiles(path)) {
                var sub = path(path, f.getName());
                if (f.isDirectory()) delete(sub);
                else client.deleteFile(sub);
              }
              client.removeDirectory(path);
            });
    }
  }

  @Override
  public InputStream read(String path) {
    if (type(path) != Type.FILE) throw new IllegalArgumentException(path + " is not a file");

    return Utils.packException(
        () -> {
          client.setFileType(FTP.BINARY_FILE_TYPE);
          var inputStream = client.retrieveFileStream(path);
          if (inputStream == null)
            throw new IllegalArgumentException("failed to open file on " + path);
          return new InputStream() {

            @Override
            public int read() throws IOException {
              return inputStream.read();
            }

            @Override
            public int read(byte b[], int off, int len) throws IOException {
              return inputStream.read(b, off, len);
            }

            @Override
            public void close() throws IOException {
              inputStream.close();
              if (!client.completePendingCommand())
                throw new IllegalStateException("Failed to complete pending command");
            }
          };
        });
  }

  @Override
  public OutputStream write(String path) {
    if (type(path) == Type.FOLDER) throw new IllegalArgumentException(path + " is a folder");
    mkdir(parent(path));
    return Utils.packException(
        () -> {
          var outputStream = client.storeFileStream(path);
          if (outputStream == null)
            throw new IllegalArgumentException("failed to create file on " + path);
          return new OutputStream() {

            @Override
            public void write(int b) throws IOException {
              outputStream.write(b);
            }

            @Override
            public void write(byte b[], int off, int len) throws IOException {
              outputStream.write(b, off, len);
            }

            @Override
            public void flush() throws IOException {
              outputStream.flush();
            }

            @Override
            public void close() throws IOException {
              outputStream.close();
              if (!client.completePendingCommand())
                throw new IllegalStateException("Failed to complete pending command");
            }
          };
        });
  }

  @Override
  public void close() {
    Utils.packException(
        () -> {
          client.logout();
          client.disconnect();
        });
  }

  private static String path(String root, String name) {
    if (root.endsWith("/")) return root + name;
    return root + "/" + name;
  }

  private static String parent(String path) {
    if (path.equals("/")) return null;
    var index = path.lastIndexOf("/");
    if (index < 0) throw new IllegalArgumentException("illegal path: " + path);
    return path.substring(0, index);
  }
}
