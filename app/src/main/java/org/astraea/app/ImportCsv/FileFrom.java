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
package org.astraea.app.ImportCsv;

import com.beust.jcommander.ParameterException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;
import org.astraea.common.Configuration;
import org.astraea.fs.FileSystem;

public interface FileFrom {
  FileSystem fileSystem();

  static FileFrom of(String source) {
    var ROOT_KEY = "fs.local.root";
    if (source.startsWith("local")) {
      var target = nonNullTarget(source).next();
      return of(FileSystem.local(Configuration.of(Map.of(ROOT_KEY, target))), target);
    } else {
      throw new ParameterException(source + " schema mismatch.");
    }
  }

  static FileFrom of(FileSystem fileSystem, String target) {
    return new FileFrom() {
      @Override
      public FileSystem fileSystem() {
        return fileSystem;
      }

      @Override
      public String toString() {
        return target;
      }
    };
  }

  private static Iterator<String> nonNullTarget(String path) {
    var iterator = Arrays.stream(path.split("://")).iterator();
    iterator.next();
    if (!iterator.hasNext()) throw new ParameterException(path + " has no target path.");
    return iterator;
  }

  class Field extends org.astraea.common.argument.Field<FileFrom> {
    static final Pattern PATTERN = Pattern.compile("^(\\w+)://.+");

    @Override
    public FileFrom convert(String path) {
      return FileFrom.of(path);
    }

    @Override
    public void check(String name, String value) {
      if (!PATTERN.matcher(value).matches()) {
        throw new ParameterException(
            "Invalid FileFrom format.Valid format example:local:///home/astraea or FileFrom://0.0.0.0");
      }
    }
  }
}
