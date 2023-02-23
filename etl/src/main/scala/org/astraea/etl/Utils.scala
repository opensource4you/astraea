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
package org.astraea.etl

import org.astraea.common.admin.Admin

import java.awt.geom.IllegalPathStateException
import java.io.File
import java.nio.file.{Files, Path}
import java.util.concurrent.CompletionStage
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.jdk.CollectionConverters._

object Utils {
  def requireFolder(path: String): Path = {
    val file = Path.of(path)
    if (!Files.isDirectory(file)) {
      throw new IllegalPathStateException(
        s"$path is not a folder. The path should be a folder."
      )
    }
    file
  }

  def requireFile(path: String): Path = {
    val file = Path.of(path)
    if (!Files.isRegularFile(file)) {
      throw new IllegalPathStateException(
        s"$path is not a file. The file does not exist."
      )
    }
    file
  }

}
