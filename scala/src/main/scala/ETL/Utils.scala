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
package ETL

import java.awt.geom.IllegalPathStateException
import java.io.File

object Utils {
  def ToDirectory(path: String): File = {
    val file = new File(path)
    if (!file.isDirectory) {
      throw new IllegalPathStateException(
        path + "is not a folder." + "The path should be a folder."
      )
    }
    file
  }

  def isPositive(number: Int): Boolean = {
    number > 0
  }
}
