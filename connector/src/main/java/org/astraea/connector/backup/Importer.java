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
package org.astraea.connector.backup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.backup.RecordReader;
import org.astraea.common.producer.Record;
import org.astraea.connector.Definition;
import org.astraea.connector.MetadataStorage;
import org.astraea.connector.SourceConnector;
import org.astraea.connector.SourceTask;
import org.astraea.fs.Type;
import org.astraea.fs.ftp.FtpFileSystem;

public class Importer extends SourceConnector {
  static Definition HOSTNAME_KEY =
      Definition.builder().name("fs.ftp.hostname").type(Definition.Type.STRING).build();
  static Definition PORT_KEY =
      Definition.builder().name("fs.ftp.port").type(Definition.Type.STRING).build();
  static Definition USER_KEY =
      Definition.builder().name("fs.ftp.user").type(Definition.Type.STRING).build();
  static Definition PASSWORD_KEY =
      Definition.builder().name("fs.ftp.password").type(Definition.Type.STRING).build();
  static Definition PATH_KEY =
      Definition.builder().name("path").type(Definition.Type.STRING).build();
  static Definition CLEAN_SOURCE_KEY =
      Definition.builder()
          .name("clean.source")
          .type(Definition.Type.STRING)
          .defaultValue("off")
          .documentation(
              "Clean source policy. Available policies: \"off\", \"delete\", \"archive\". Default: off")
          .build();
  static Definition ARCHIVE_DIR_KEY =
      Definition.builder().name("archive.dir").type(Definition.Type.STRING).build();
  public static final String FILE_SET_KEY = "file.set";
  private Configuration config;
  private FtpFileSystem fs;
  private String input;
  private String cleanSource;
  private Optional<String> archiveDir;

  @Override
  protected void init(Configuration configuration, MetadataStorage storage) {
    this.config = configuration;
    this.fs = new FtpFileSystem(config);
    this.input = config.requireString(PATH_KEY.name());
    this.cleanSource =
        config.string(CLEAN_SOURCE_KEY.name()).orElse(CLEAN_SOURCE_KEY.defaultValue().toString());
    this.archiveDir = config.string("archive.dir");
  }

  @Override
  protected Class<? extends SourceTask> task() {
    return Task.class;
  }

  @Override
  protected List<Configuration> takeConfiguration(int maxTasks) {
    return IntStream.range(0, maxTasks)
        .mapToObj(
            i -> {
              var taskMap = new HashMap<>(config.raw());
              taskMap.put(FILE_SET_KEY, String.valueOf(i));
              return Configuration.of(taskMap);
            })
        .collect(Collectors.toList());
  }

  @Override
  protected void close() {
    switch (cleanSource) {
      case "off":
        break;
      case "delete":
        fs.delete(input);
        break;
      case "archive":
        fs.rename(input, archiveDir.get());
        break;
    }
    this.fs.close();
  }

  @Override
  protected List<Definition> definitions() {
    return List.of(
        HOSTNAME_KEY,
        PORT_KEY,
        USER_KEY,
        PASSWORD_KEY,
        PATH_KEY,
        CLEAN_SOURCE_KEY,
        ARCHIVE_DIR_KEY);
  }

  public static class Task extends SourceTask {
    private FtpFileSystem fs;
    private int fileSet;
    private HashSet<String> addedPath;
    private String input;
    private int maxTask;
    private LinkedList<String> paths;

    protected void init(Configuration configuration) {
      this.fs = new FtpFileSystem(configuration);
      this.fileSet = configuration.requireInteger(FILE_SET_KEY);
      this.addedPath = new HashSet<>();
      this.input = configuration.requireString(PATH_KEY.name());
      this.maxTask = configuration.requireInteger("tasks.max");
      this.paths = new LinkedList<>();
    }

    @Override
    protected Collection<Record<byte[], byte[]>> take() {
      getFileSet();
      var currentPath = paths.poll();
      if (currentPath != null) {
        var records = new ArrayList<Record<byte[], byte[]>>();
        var inputStream = fs.read(currentPath);
        var reader = RecordReader.builder(inputStream).build();
        while (reader.hasNext()) {
          var record = reader.next();
          if (record.key() == null && record.value() == null) continue;
          records.add(
              Record.builder()
                  .topic(record.topic())
                  .partition(record.partition())
                  .key(record.key())
                  .value(record.value())
                  .timestamp(record.timestamp())
                  .headers(record.headers())
                  .build());
        }
        Utils.packException(inputStream::close);
        return records;
      }
      return null;
    }

    protected void getFileSet() {
      if (paths.isEmpty()) {
        LinkedList<String> path = new LinkedList<>(Collections.singletonList(input));
        while (true) {
          var current = path.poll();
          if (current == null) break;
          if (fs.type(current) == Type.FOLDER) {
            var files = fs.listFiles(current);
            var folders = fs.listFolders(current);
            if (!files.isEmpty()) {
              files.stream()
                  .filter(file -> (file.hashCode() & Integer.MAX_VALUE) % maxTask == fileSet)
                  .filter(Predicate.not(file -> addedPath.contains(file)))
                  .forEach(file -> paths.add(file));
              continue;
            }
            path.addAll(folders);
          }
        }
      }
      addedPath.addAll(paths);
    }

    @Override
    protected void close() {
      this.fs.close();
    }
  }
}
