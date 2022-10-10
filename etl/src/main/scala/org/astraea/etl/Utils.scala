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

import java.awt.geom.IllegalPathStateException
import java.io.File
import java.util.concurrent.CompletionStage
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

object Utils {
  def requireFolder(path: String): File = {
    val file = new File(path)
    if (!file.isDirectory) {
      throw new IllegalPathStateException(
        s"$path is not a folder. The path should be a folder."
      )
    }
    file
  }

  def requireFile(path: String): File = {
    val file = new File(path)
    if (!file.exists()) {
      throw new IllegalPathStateException(
        s"$path is not a file. The file does not exist."
      )
    }
    file
  }

  /** Lack of means of try with resource in scala 2.12.So replace it with the
    * following method.
    */
  def Using[T <: AutoCloseable, V](r: => T)(f: T => V): V = {
    val resource: T = r
    require(resource != null, "resource is null")
    var exception: Throwable = null
    try f(resource)
    catch {
      case NonFatal(e) =>
        exception = e
        throw e
    } finally closeAndAddSuppressed(exception, resource)
  }

  /** convert java future to scala in scala 2.12.
    */
  def asScala[T](f: CompletionStage[T]): Future[T] = {
    val promise = Promise[T]
    f.whenComplete { (r, e) =>
      if (e != null) promise.failure(e) else promise.success(r)
    }
    promise.future
  }

  private def closeAndAddSuppressed(
      e: Throwable,
      resource: AutoCloseable
  ): Unit = {
    if (e != null) {
      try {
        resource.close()
      } catch {
        case NonFatal(suppressed) =>
          e.addSuppressed(suppressed)
      }
    } else {
      resource.close()
    }
  }
}
