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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.fs.FileSystem;
import org.astraea.fs.Type;

public class HdfsFileSystem implements FileSystem {

  public static final String HOSTNAME_KEY = "fs.hdfs.hostname";
  public static final String PORT_KEY = "fs.hdfs.port";
  public static final String USER_KEY = "fs.hdfs.user";
  public static final String OVERRIDE_KEY = "fs.hdfs.override";

  private final org.apache.hadoop.fs.FileSystem fs;

  static org.apache.hadoop.fs.FileSystem create(Configuration config) {
    return Utils.packException(
        () -> {
          var uri =
              new URI(
                  "hdfs://"
                      + config.requireString(HOSTNAME_KEY)
                      + ":"
                      + config.requireString(PORT_KEY));

          var conf = new org.apache.hadoop.conf.Configuration();

          config
              .filteredPrefixConfigs(OVERRIDE_KEY)
              .entrySet()
              .forEach(configItem -> conf.set(configItem.getKey(), configItem.getValue()));

          return org.apache.hadoop.fs.FileSystem.get(uri, conf, config.requireString(USER_KEY));
        });
  }

  public HdfsFileSystem(Configuration config) {
    fs = create(config);
  }

  @Override
  public Type type(String path) {
    return Utils.packException(
        () -> {
          var targetPath = new Path(path);
          if (!fs.exists(targetPath)) return Type.NONEXISTENT;
          if (fs.getFileStatus(targetPath).isDirectory()) return Type.FOLDER;
          return Type.FILE;
        });
  }

  @Override
  public void mkdir(String path) {
    Utils.packException(
        () -> {
          if (type(path) == Type.FOLDER) return;
          if (!fs.mkdirs(new Path(path)))
            throw new IllegalArgumentException("Failed to create folder on " + path);
        });
  }

  @Override
  public List<String> listFiles(String path) {
    return Utils.packException(
        () -> {
          if (type(path) != Type.FOLDER)
            throw new IllegalArgumentException(path + " is not a folder");
          return Arrays.stream(fs.listStatus(new Path(path)))
              .filter(FileStatus::isFile)
              .map(f -> f.getPath().toUri().getPath())
              .collect(Collectors.toList());
        });
  }

  @Override
  public List<String> listFolders(String path) {
    return Utils.packException(
        () -> {
          if (type(path) != Type.FOLDER)
            throw new IllegalArgumentException(path + " is not a folder");
          return Arrays.stream(fs.listStatus(new Path(path)))
              .filter(FileStatus::isDirectory)
              .map(f -> FileSystem.path(path, f.getPath().getName()))
              .collect(Collectors.toList());
        });
  }

  @Override
  public void delete(String path) {
    Utils.packException(
        () -> {
          if (path.equals("/"))
            throw new IllegalArgumentException("Can't delete while root folder");
          if (type(path) == Type.NONEXISTENT) return;
          fs.delete(new Path(path), true);
        });
  }

  @Override
  public InputStream read(String path) {
    if (type(path) != Type.FILE) throw new IllegalArgumentException(path + " is not a file");
    try {
      return fs.open(new Path(path));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public OutputStream write(String path) {
    if (type(path) == Type.FOLDER) throw new IllegalArgumentException(path + " is a folder");
    try {
      return fs.create(new Path(path), true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    Utils.packException(fs::close);
  }
}
