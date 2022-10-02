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

sealed abstract class DeployModePattern(pattern: Regex) {
  def r: Regex = {
    pattern
  }
}

object DeployModePattern {
  // Whether it is a local mode string.
  case object LocalModePattern
      extends DeployModePattern("local(\\[)[\\d{1}](])".r)
  // Whether it is a standalone mode string.
  case object StandAloneModePattern
      extends DeployModePattern("spark://(.+):(\\d+)".r)

  def of(str: String): Boolean = {
    all().exists(_.r matches str)
  }

  def all() = {
    Seq(LocalModePattern, StandAloneModePattern)
  }
}
