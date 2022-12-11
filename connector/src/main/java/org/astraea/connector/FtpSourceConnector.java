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
package org.astraea.connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.astraea.common.Configuration;
import org.astraea.fs.Type;
import org.astraea.fs.ftp.FtpFileSystem;

public class FtpSourceConnector extends SourceConnector {
  private Configuration config;
  private FtpFileSystem fs;
  private LinkedList<String> path;
  private String input;
  private boolean cleanSource;
  private Optional<String> archiveDir;

  @Override
  protected void init(Configuration configuration) {
    this.config = configuration;
    this.fs = new FtpFileSystem(config);
    this.path = new LinkedList<>();
    this.input = config.requireString("input");
    path.add(input);
    this.cleanSource = Boolean.parseBoolean(configuration.requireString("clean.source"));
    this.archiveDir = Optional.ofNullable(configuration.requireString("archive.dir"));
  }

  @Override
  protected Class<? extends SourceTask> task() {
    return FtpSourceTask.class;
  }

  @Override
  protected List<Configuration> takeConfiguration(int maxTasks) {
    var configs = new ArrayList<Configuration>();
    var paths = new ArrayList<String>();
    while (true) {
      var current = path.poll();
      if (current == null) break;
      if (fs.type(current) == Type.FOLDER) {
        var files = fs.listFiles(current);
        var folders = fs.listFolders(current);
        if (folders.isEmpty()) {
          if (!files.isEmpty()) paths.add(current);
          continue;
        }
        path.addAll(folders);
      }
    }
    // assign partition folder as input into each task's config.
    avgAssign(paths, Math.min(paths.size(), maxTasks))
        .forEach(
            path -> {
              var taskMap = new HashMap<>(config.raw());
              taskMap.put("input", path);
              configs.add(Configuration.of(taskMap));
            });
    return configs;
  }

  private List<String> avgAssign(List<String> source, int n) {
    var result = new ArrayList<String>();
    int remainder = source.size() % n;
    int number = source.size() / n;
    int offset = 0;
    for (int i = 0; i < n; i++) {
      List<String> value;
      if (remainder > 0) {
        value = source.subList(i * number + offset, (i + 1) * number + offset + 1);
        remainder--;
        offset++;
      } else {
        value = source.subList(i * number + offset, (i + 1) * number + offset);
      }
      result.add(String.join(",", value));
    }
    return result;
  }

  @Override
  protected void close() {
    if (archiveDir.isPresent()) {
      fs.rename(input, archiveDir.get());
      return;
    }
    if (cleanSource) fs.delete(input);
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
}
