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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
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
          if (!Files.isDirectory(Path.of(r)))
            throw new IllegalArgumentException("the root:" + r + " is not folder");
        });
  }

  private Path resolvePath(String path) {
    return Path.of(root.map(r -> FileSystem.path(r, path)).orElse(path));
  }

  @Override
  public synchronized void mkdir(String path) {
    var folder = resolvePath(path);
    if (Files.isRegularFile(folder)) throw new IllegalArgumentException(path + " is a file");
    if (Files.isDirectory(folder)) return;
    Utils.packException(() -> Files.createDirectories(folder));
  }

  @Override
  public synchronized List<String> listFiles(String path) {
    return listFolders(path, true);
  }

  @Override
  public synchronized List<String> listFolders(String path) {
    return listFolders(path, false);
  }

  private synchronized List<String> listFolders(String path, boolean requireFile) {
    var folder = resolvePath(path);
    System.out.println("[chia] " + folder);
    if (!Files.isDirectory(folder)) throw new IllegalArgumentException(path + " is not a folder");
    return Utils.packException(() -> Files.list(folder))
        .filter(f -> requireFile ? Files.isRegularFile(f) : Files.isDirectory(f))
        .map(Path::toAbsolutePath)
        .map(
            p ->
                Path.of("/")
                    .resolve(
                        root.map(r -> p.subpath(Path.of(r).getNameCount(), p.getNameCount()))
                            .orElse(p))
                    .toString())
        .collect(Collectors.toList());
  }

  @Override
  public synchronized void delete(String path) {
    if (Path.of(path).getNameCount() == 0)
      throw new IllegalArgumentException("can't delete whole root folder");
    var resolvedPath = resolvePath(path);
    if (Files.notExists(resolvedPath)) return;
    Utils.packException(
        () ->
            Files.walkFileTree(
                resolvedPath,
                new SimpleFileVisitor<>() {
                  @Override
                  public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                      throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                  }

                  @Override
                  public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                      throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                  }
                }));
  }

  @Override
  public synchronized InputStream read(String path) {
    return Utils.packException(
        () -> {
          if (type(path) != Type.FILE) throw new IllegalArgumentException(path + " is not a file");
          return Files.newInputStream(resolvePath(path));
        });
  }

  @Override
  public synchronized OutputStream write(String path) {
    return Utils.packException(
        () -> {
          if (type(path) == Type.FOLDER) throw new IllegalArgumentException(path + " is a folder");
          FileSystem.parent(path).ifPresent(this::mkdir);
          return Files.newOutputStream(resolvePath(path));
        });
  }

  @Override
  public synchronized Type type(String path) {
    var f = resolvePath(path);
    if (Files.notExists(f)) return Type.NONEXISTENT;
    if (Files.isDirectory(f)) return Type.FOLDER;
    return Type.FILE;
  }

  @Override
  public void close() {
    // empty
  }
}
