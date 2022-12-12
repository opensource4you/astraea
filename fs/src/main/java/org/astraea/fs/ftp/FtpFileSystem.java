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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.fs.FileSystem;
import org.astraea.fs.Type;

public class FtpFileSystem implements FileSystem {
  public static final String HOSTNAME_KEY = "fs.ftp.hostname";
  public static final String PORT_KEY = "fs.ftp.port";
  public static final String USER_KEY = "fs.ftp.user";
  public static final String PASSWORD_KEY = "fs.ftp.password";

  private final Supplier<FTPClient> clientSupplier;
  private final FTPClient client;

  public FtpFileSystem(Configuration config) {
    clientSupplier =
        () ->
            Utils.packException(
                () -> {
                  var client = new FTPClient();
                  client.connect(
                      config.requireString(HOSTNAME_KEY), config.requireInteger(PORT_KEY));
                  client.enterLocalPassiveMode();
                  // the data connection can be different from control connection
                  client.setRemoteVerificationEnabled(false);
                  if (!client.login(
                      config.requireString(USER_KEY), config.requireString(PASSWORD_KEY)))
                    throw new IllegalArgumentException("failed to login ftp server");
                  return client;
                });
    client = clientSupplier.get();
  }

  @Override
  public synchronized Type type(String path) {
    return Utils.packException(
        () -> {
          // tyring to fit different type of ftp servers.
          var parentPath = path.substring(0, path.lastIndexOf("/"));
          if (Arrays.stream(client.listFiles(parentPath))
              .filter(FTPFile::isFile)
              .map(FTPFile::getName)
              .anyMatch(path.substring(path.lastIndexOf("/") + 1)::equals)) {
            return Type.FILE;
          }

          var currentPath = client.printWorkingDirectory();
          client.changeWorkingDirectory(path);
          try {
            if (client.getReplyCode() == 550) return Type.NONEXISTENT;
            return Type.FOLDER;
          } finally {
            // keep the working directory in its original location
            // to prevent any unexpected errors.
            client.changeWorkingDirectory(currentPath);
          }
        });
  }

  @Override
  public synchronized void mkdir(String path) {
    Utils.packException(
        () -> {
          if (type(path) == Type.FOLDER) return;
          FileSystem.parent(path).filter(p -> type(p) == Type.NONEXISTENT).ifPresent(this::mkdir);
          if (!client.changeWorkingDirectory(path) && !client.makeDirectory(path))
            throw new IllegalArgumentException("Failed to create folder on " + path);
        });
  }

  @Override
  public synchronized List<String> listFiles(String path) {
    return Utils.packException(
        () -> {
          if (type(path) != Type.FOLDER)
            throw new IllegalArgumentException(path + " is not a folder");
          return Arrays.stream(client.listFiles(path, FTPFile::isFile))
              .map(f -> FileSystem.path(path, f.getName()))
              .collect(Collectors.toList());
        });
  }

  @Override
  public synchronized List<String> listFolders(String path) {
    return Utils.packException(
        () -> {
          if (type(path) != Type.FOLDER)
            throw new IllegalArgumentException(path + " is not a folder");
          return Arrays.stream(client.listFiles(path, FTPFile::isDirectory))
              .map(f -> FileSystem.path(path, f.getName()))
              .collect(Collectors.toList());
        });
  }

  @Override
  public synchronized void delete(String path) {
    Utils.packException(
        () -> {
          if (path.equals("/"))
            throw new IllegalArgumentException("Can't delete whole root folder");
          switch (type(path)) {
            case NONEXISTENT:
              return;
            case FILE:
              client.deleteFile(path);
              return;
            case FOLDER:
              for (var f : client.listFiles(path)) {
                var sub = FileSystem.path(path, f.getName());
                if (f.isDirectory()) delete(sub);
                else client.deleteFile(sub);
              }
              client.removeDirectory(path);
          }
        });
  }

  public synchronized void rename(String from, String to) {
    Utils.packException(
        () -> {
          var client = clientSupplier.get();
          client.rename(from, to);
          FtpFileSystem.close(client);
        });
  }

  @Override
  public synchronized InputStream read(String path) {
    return Utils.packException(
        () -> {
          if (type(path) != Type.FILE) throw new IllegalArgumentException(path + " is not a file");
          // FTPClient can handle only one data connection, so we have to create another client to
          // handle input stream
          // see https://lists.apache.org/thread/7pjjw8bb1qo9noz3dcxkdcr6v7kx8c1l
          var client = clientSupplier.get();
          client.setFileType(FTP.BINARY_FILE_TYPE);
          var inputStream = client.retrieveFileStream(path);
          if (inputStream == null) {
            FtpFileSystem.close(client);
            throw new IllegalArgumentException("failed to open file on " + path);
          }
          return new InputStream() {

            @Override
            public int read() throws IOException {
              return inputStream.read();
            }

            @Override
            public int read(byte b[], int off, int len) throws IOException {
                var count = inputStream.read(b, off, len);
                while (count != len) {
                    var offset = inputStream.read(b, count, len - count);
                    count += offset;
                }
                return count;
            }

            @Override
            public void close() throws IOException {
              inputStream.close();
              if (!client.completePendingCommand())
                throw new IllegalStateException("Failed to complete pending command");
              FtpFileSystem.close(client);
            }
          };
        });
  }

  @Override
  public synchronized OutputStream write(String path) {
    return Utils.packException(
        () -> {
          if (type(path) == Type.FOLDER) throw new IllegalArgumentException(path + " is a folder");
          FileSystem.parent(path).ifPresent(this::mkdir);
          // FTPClient can handle only one data connection, so we have to create another client to
          // handle output stream
          // see https://lists.apache.org/thread/7pjjw8bb1qo9noz3dcxkdcr6v7kx8c1l
          var client = clientSupplier.get();
          client.setFileType(FTP.BINARY_FILE_TYPE);
          var outputStream = client.storeFileStream(path);
          if (outputStream == null) {
            FtpFileSystem.close(client);
            throw new IllegalArgumentException("failed to create file on " + path);
          }
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
              FtpFileSystem.close(client);
            }
          };
        });
  }

  @Override
  public void close() {
    close(client);
  }

  private static void close(FTPClient client) {
    Utils.packException(
        () -> {
          client.logout();
          client.disconnect();
        });
  }
}
