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
  private Configuration config;
  private FtpFileSystem fs;
  private String input;
  private boolean cleanSource;
  private Optional<String> archiveDir;

  @Override
  protected void init(Configuration configuration, MetadataStorage storage) {
    this.config = configuration;
    this.fs = new FtpFileSystem(config);
    this.input = config.requireString("input");
    this.cleanSource = Boolean.parseBoolean(configuration.requireString("clean.source"));
    this.archiveDir = Optional.ofNullable(configuration.requireString("archive.dir"));
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
              taskMap.put("task.id", String.valueOf(i));
              return Configuration.of(taskMap);
            })
        .collect(Collectors.toList());
  }

  @Override
  protected void close() {
    if (archiveDir.isPresent()) fs.rename(input, archiveDir.get());
    else if (cleanSource) fs.delete(input);
    this.fs.close();
  }

  @Override
  protected List<Definition> definitions() {
    return List.of(
        Definition.builder()
            .name("fs.ftp.hostname")
            .type(Definition.Type.STRING)
            .defaultValue(null)
            .documentation("ftp host.")
            .build());
  }

  public static class Task extends SourceTask {
    private FtpFileSystem fs;
    private int id;
    private HashSet<String> addedPath;
    private String input;
    private int maxTask;
    private LinkedList<String> paths;

    protected void init(Configuration configuration) {
      this.fs = new FtpFileSystem(configuration);
      this.id = configuration.requireInteger("task.id");
      this.addedPath = new HashSet<>();
      this.input = configuration.requireString("input");
      this.maxTask = configuration.requireInteger("tasks.max");
      this.paths = new LinkedList<>();
    }

    @Override
    protected Collection<Record<byte[], byte[]>> take() {
      if (paths.isEmpty()) {
        LinkedList<String> path = new LinkedList<>(Collections.singletonList(input));
        while (true) {
          var current = path.poll();
          if (current == null) break;
          if (fs.type(current) == Type.FOLDER) {
            var files = fs.listFiles(current);
            var folders = fs.listFolders(current);
            if (folders.isEmpty()) {
              if (!files.isEmpty() && (current.hashCode() & Integer.MAX_VALUE) % maxTask == id) {
                paths.addAll(
                    files.stream()
                        .filter(Predicate.not(file -> addedPath.contains(file)))
                        .collect(Collectors.toList()));
                continue;
              }
            }
            path.addAll(folders);
          }
        }
      }
      addedPath.addAll(paths);
      if (!paths.isEmpty()) {
        var records = new ArrayList<Record<byte[], byte[]>>();
        var currentPath = paths.poll();
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
        System.out.println(
            "succeed to add "
                + records.size()
                + " records from "
                + currentPath
                + " from task "
                + id);
        Utils.packException(inputStream::close);
        return records;
      }
      return null;
    }

    @Override
    protected void close() {
      this.fs.close();
    }
  }
}
