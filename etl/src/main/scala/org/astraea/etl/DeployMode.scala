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

import scala.util.matching.Regex

sealed abstract class DeployMode(pattern: Regex) {
  def r: Regex = {
    pattern
  }
}

object DeployMode {
  // Whether it is a local mode string.
  case object Local extends DeployMode("local(\\[)[\\d{1}](])".r)
  // Whether it is a standalone mode string.
  case object Standalone extends DeployMode("spark://(.+)".r)

  def of(str: String): DeployMode = {
    val patterns = all().filter(_.r.findAllIn(str).hasNext)
    if (patterns.isEmpty) {
      throw new IllegalArgumentException(
        s"$str does not belong to any of the deploy modes."
      )
    }
    patterns.head
  }

  def deployMatch(str: String): Boolean = {
    all().exists(_.r.findAllIn(str).hasNext)
  }

  def all(): Seq[DeployMode] = {
    Seq(Local, Standalone)
  }
}
