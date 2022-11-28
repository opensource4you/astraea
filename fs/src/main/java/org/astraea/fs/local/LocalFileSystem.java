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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.fs.FileSystem;
import org.astraea.fs.Type;

public class LocalFileSystem implements FileSystem {

  // isolate all files on the given path;
  static final String ROOT_KEY = "fs.local.root";

  private final Optional<String> root;

  public LocalFileSystem(Configuration configuration) {
    root = configuration.string(ROOT_KEY).filter(r -> !r.equals("/"));
    root.ifPresent(
        r -> {
          var f = new File(r);
          if (!f.isDirectory())
            throw new IllegalArgumentException("the root:" + r + " is not folder");
        });
  }

  private File resolvePath(String path) {
    return new File(root.map(r -> FileSystem.path(r, path)).orElse(path));
  }

  @Override
  public void mkdir(String path) {
    var folder = resolvePath(path);
    if (folder.isFile()) throw new IllegalArgumentException(path + " is a file");
    if (folder.isDirectory()) return;
    if (!folder.mkdirs()) throw new IllegalArgumentException("Failed to create folder on " + path);
  }

  @Override
  public List<String> listFiles(String path) {
    var folder = resolvePath(path);
    if (!folder.isDirectory()) throw new IllegalArgumentException(path + " is not a folder");
    var fs = folder.listFiles(File::isFile);
    if (fs == null) throw new IllegalArgumentException("failed to list files on " + path);
    return Arrays.stream(fs)
        .map(File::getAbsolutePath)
        .map(p -> root.map(r -> p.substring(p.indexOf(r) + r.length())).orElse(p))
        .collect(Collectors.toList());
  }

  @Override
  public List<String> listFolders(String path) {
    var folder = resolvePath(path);
    if (!folder.isDirectory()) throw new IllegalArgumentException(path + " is not a folder");
    var fs = folder.listFiles(File::isDirectory);
    if (fs == null) throw new IllegalArgumentException("failed to list directory on " + path);
    return Arrays.stream(fs)
        .map(File::getAbsolutePath)
        .map(p -> root.map(r -> p.substring(p.indexOf(r) + r.length())).orElse(p))
        .collect(Collectors.toList());
  }

  @Override
  public void delete(String path) {
    if (path.equals("/")) throw new IllegalArgumentException("can't delete whole root folder");
    delete(path, resolvePath(path));
  }

  private void delete(String root, File file) {
    if (!file.exists()) return;
    if (file.isFile() && !file.delete())
      throw new IllegalArgumentException("failed to delete " + root);
    if (file.isDirectory()) {
      var fs = file.listFiles();
      if (fs != null) Arrays.stream(fs).forEach(f -> delete(root, f));
      if (!file.delete()) throw new IllegalArgumentException("failed to delete " + root);
    }
  }

  @Override
  public InputStream read(String path) {
    if (type(path) != Type.FILE) throw new IllegalArgumentException(path + " is not a file");
    try {
      return new FileInputStream(resolvePath(path));
    } catch (FileNotFoundException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public OutputStream write(String path) {
    if (type(path) == Type.FOLDER) throw new IllegalArgumentException(path + " is a folder");
    mkdir(FileSystem.parent(path));
    try {
      return new FileOutputStream(resolvePath(path));
    } catch (FileNotFoundException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public Type type(String path) {
    var f = resolvePath(path);
    if (!f.exists()) return Type.NONEXISTENT;
    if (f.isDirectory()) return Type.FOLDER;
    return Type.FILE;
  }

  @Override
  public void close() {
    // empty
  }
}
