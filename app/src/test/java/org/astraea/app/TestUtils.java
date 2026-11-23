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
package org.astraea.app;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.astraea.app.web.ResponseTest;
import org.astraea.common.Utils;

public class TestUtils {

  private TestUtils() {}

  public static List<Class<?>> getProductionClass() {
    var pkg = "org/astraea";
    // the app classpath carries the main output of every module, so all of them get scanned
    var mainDirs =
        Collections.list(
                Utils.packException(() -> ResponseTest.class.getClassLoader().getResources(pkg)))
            .stream()
            .filter(x -> x.toExternalForm().contains("main/" + pkg))
            .map(x -> Utils.packException(() -> Path.of(x.toURI())))
            .map(x -> x.resolve("../../").normalize())
            .toList();

    var classNames =
        mainDirs.stream()
            .flatMap(
                dir ->
                    FileUtils.listFiles(dir.toFile(), new String[] {"class"}, true).stream()
                        .map(file -> dir.relativize(file.toPath())))
            .map(path -> FilenameUtils.removeExtension(path.toString()))
            .map(name -> name.replace(File.separatorChar, '.'))
            .toList();

    return classNames.stream()
        .map(x -> Utils.packException(() -> Class.forName(x)))
        .collect(Collectors.toList());
  }
}
