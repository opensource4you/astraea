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

import org.astraea.etl.Metadata.requirePair

case class DataColumn(name: String, isPK: Boolean, dataType: DataType)

object DataColumn {
  private[this] val COLUMN_NAME = "column.name"
  private[this] val PRIMARY_KEYS = "primary.keys"

  def columnParse(cols: String, pk: String): Seq[DataColumn] = {
    val colsMap = requireNonidentical(COLUMN_NAME, cols)
    val pkMap = requireNonidentical(PRIMARY_KEYS, pk)
    val colsArray = colsMap.keys.toArray
    val combine = pkMap.keys.toArray ++ colsArray
    if (combine.distinct.length != colsMap.size) {
      val column = colsMap.keys.toArray
      throw new IllegalArgumentException(
        s"The ${combine
          .diff(column)
          .mkString(
            PRIMARY_KEYS + "(",
            ", ",
            ")"
          )} not in column. All $PRIMARY_KEYS should be included in the column."
      )
    }
    val colsSeq = colsMap
      .map(entry =>
        DataColumn(
          entry._1,
          isPK = pkMap.contains(entry._1),
          DataType.of(entry._2)
        )
      )
      .toSeq
    colsSeq
  }

  //No duplicate values should be set.
  def requireNonidentical(
      string: String,
      prop: String
  ): Map[String, String] = {
    val array = prop.split(",")
    val map = requirePair(prop)
    if (map.size != array.length) {
      val column = map.keys.toArray
      throw new IllegalArgumentException(
        s"${array
          .diff(column)
          .mkString(
            string + " (",
            ", ",
            ")"
          )} is duplication. The $string should not be duplicated."
      )
    }
    map
  }
}
