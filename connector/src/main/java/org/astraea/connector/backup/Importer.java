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
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.backup.RecordReader;
import org.astraea.common.backup.RecordWriter;
import org.astraea.connector.Definition;
import org.astraea.connector.MetadataStorage;
import org.astraea.connector.SourceConnector;
import org.astraea.connector.SourceRecord;
import org.astraea.connector.SourceTask;
import org.astraea.fs.FileSystem;
import org.astraea.fs.Type;

public class Importer extends SourceConnector {
  static Definition SCHEMA_KEY =
      Definition.builder()
          .name("fs.schema")
          .type(Definition.Type.STRING)
          .documentation("decide which file system to use, such as local, ftp, hdfs.")
          .required()
          .build();
  static Definition HOSTNAME_KEY =
      Definition.builder()
          .name("fs.<schema>.hostname")
          .type(Definition.Type.STRING)
          .documentation("the host name of the <schema> server used.")
          .build();
  static Definition PORT_KEY =
      Definition.builder()
          .name("fs.<schema>.port")
          .type(Definition.Type.STRING)
          .documentation("the port of the <schema> server used.")
          .build();
  static Definition USER_KEY =
      Definition.builder()
          .name("fs.<schema>.user")
          .type(Definition.Type.STRING)
          .documentation("the user name required to login to the <schema> server.")
          .build();
  static Definition PASSWORD_KEY =
      Definition.builder()
          .name("fs.<schema>.password")
          .type(Definition.Type.STRING)
          .documentation("the password required to login to the <schema> server.")
          .build();
  static Definition PATH_KEY =
      Definition.builder()
          .name("path")
          .type(Definition.Type.STRING)
          .documentation("The root directory of the file that needs to be imported.")
          .required()
          .build();
  static String CLEAN_SOURCE_DEFAULT = "off";
  static Definition CLEAN_SOURCE_KEY =
      Definition.builder()
          .name("clean.source")
          .type(Definition.Type.STRING)
          .defaultValue(CLEAN_SOURCE_DEFAULT)
          .documentation(
              "Clean source policy. Available policies: \"off\", \"delete\", \"archive\". Default: off")
          .build();
  static Definition ARCHIVE_DIR_KEY =
      Definition.builder()
          .name("archive.dir")
          .type(Definition.Type.STRING)
          .documentation("the directory of the imported file that needs to be archived")
          .build();
  public static final String FILE_SET_KEY = "file.set";
  public static final String TASKS_COUNT_KEY = "tasks.count";
  private Configuration config;

  @Override
  protected void init(Configuration configuration, MetadataStorage storage) {
    this.config = configuration;
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
              taskMap.put(TASKS_COUNT_KEY, String.valueOf(maxTasks));
              return new Configuration(taskMap);
            })
        .toList();
  }

  @Override
  protected List<Definition> definitions() {
    return List.of(
        SCHEMA_KEY,
        HOSTNAME_KEY,
        PORT_KEY,
        USER_KEY,
        PASSWORD_KEY,
        PATH_KEY,
        CLEAN_SOURCE_KEY,
        ARCHIVE_DIR_KEY);
  }

  public static class Task extends SourceTask {
    private FileSystem Client;
    private int fileSet;
    private Set<String> addedPaths;
    private String rootDir;
    private int tasksCount;
    private List<String> paths;
    private String cleanSource;
    private Optional<String> archiveDir;

    protected void init(Configuration configuration, MetadataStorage storage) {
      this.Client = FileSystem.of(configuration.requireString(SCHEMA_KEY.name()), configuration);
      this.fileSet = configuration.requireInteger(FILE_SET_KEY);
      this.addedPaths = new HashSet<>();
      this.rootDir = configuration.requireString(PATH_KEY.name());
      this.tasksCount = configuration.requireInteger(TASKS_COUNT_KEY);
      this.paths = new LinkedList<>();
      this.cleanSource = configuration.string(CLEAN_SOURCE_KEY.name()).orElse(CLEAN_SOURCE_DEFAULT);
      this.archiveDir = configuration.string(ARCHIVE_DIR_KEY.name());
    }

    @Override
    protected Collection<SourceRecord> take() {
      if (paths.isEmpty()) {
        paths = getFileSet(addedPaths, rootDir, tasksCount, fileSet);
      }
      addedPaths.addAll(paths);
      var currentPath = ((LinkedList<String>) paths).poll();
      if (currentPath != null) {
        var records = new ArrayList<SourceRecord>();
        var inputStream = Client.read(currentPath);
        var reader = RecordReader.builder(inputStream).build();
        while (reader.hasNext()) {
          var record = reader.next();
          if (record.key() == null && record.value() == null) continue;
          records.add(
              SourceRecord.builder()
                  .topic(record.topic())
                  .partition(record.partition())
                  .key(record.key())
                  .value(record.value())
                  .timestamp(record.timestamp())
                  .headers(record.headers())
                  .build());
        }
        Utils.close(inputStream);
        switch (cleanSource) {
          case "archive":
            var archiveInput = Client.read(currentPath);
            var archiveReader = RecordReader.builder(archiveInput).build();
            var archiveWriter =
                RecordWriter.builder(
                        Client.write(currentPath.replaceFirst(rootDir, archiveDir.get())))
                    .build();
            while (archiveReader.hasNext()) {
              var record = archiveReader.next();
              if (record.key() == null && record.value() == null) continue;
              archiveWriter.append(record);
            }
            archiveWriter.close();
            Utils.close(archiveInput);
          case "delete":
            Client.delete(currentPath);
          case "off":
            break;
        }
        return records;
      }
      return null;
    }

    protected LinkedList<String> getFileSet(
        Set<String> addedPaths, String root, int TasksCount, int fileSet) {
      var filePaths = new LinkedList<String>();
      var path = new LinkedList<>(Collections.singletonList(root));
      while (true) {
        var current = path.poll();
        if (current == null) break;
        if (Client.type(current) == Type.FOLDER) {
          var files = Client.listFiles(current);
          var folders = Client.listFolders(current);
          if (!files.isEmpty()) {
            files.stream()
                .filter(file -> (file.hashCode() & Integer.MAX_VALUE) % TasksCount == fileSet)
                .filter(Predicate.not(addedPaths::contains))
                .forEach(filePaths::add);
            continue;
          }
          path.addAll(folders);
        }
      }
      return filePaths;
    }

    @Override
    protected void close() {
      this.Client.close();
    }
  }
}
