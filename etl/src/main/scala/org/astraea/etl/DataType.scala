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

sealed abstract class DataType(dataType: String) {
  def value: String = {
    dataType
  }
}

/** Column Types supported by astraea dispatcher. */
object DataType {
  private val STRING_TYPE = "string"
  private val BINARY_TYPE = "binary"
  private val BOOLEAN_TYPE = "boolean"
  private val DATE_TYPE = "date"
  private val DOUBLE_TYPE = "double"
  private val BYTE_TYPE = "byte"
  private val INTEGER_TYPE = "integer"
  private val LONG_TYPE = "long"
  private val SHORT_TYPE = "short"

  case object StringType extends DataType(STRING_TYPE)
  case object BinaryType extends DataType(BINARY_TYPE)
  case object BooleanType extends DataType(BOOLEAN_TYPE)
  case object DateType extends DataType(DATE_TYPE)
  case object DoubleType extends DataType(DOUBLE_TYPE)
  case object ByteType extends DataType(BYTE_TYPE)
  case object IntegerType extends DataType(INTEGER_TYPE)
  case object LongType extends DataType(LONG_TYPE)
  case object ShortType extends DataType(SHORT_TYPE)

  /** @param str
    *   String that needs to be parsed as a DataType.
    * @return
    *   DataType
    */
  def parseDataType(str: String): DataType = {
    str match {
      case STRING_TYPE =>
        StringType
      case BINARY_TYPE =>
        BinaryType
      case BOOLEAN_TYPE =>
        BooleanType
      case DATE_TYPE =>
        DateType
      case DOUBLE_TYPE =>
        DoubleType
      case BYTE_TYPE =>
        ByteType
      case INTEGER_TYPE =>
        IntegerType
      case LONG_TYPE =>
        LongType
      case SHORT_TYPE =>
        ShortType
      case _ =>
        throw new IllegalArgumentException(
          s"$str is not supported data type.The data types supported ${allTypes.mkString(",")}"
        )
    }
  }

  /** @param map
    *   A set of string needs to be parsed as DataType
    * @return
    *   Map[String, DataType]
    */
  def parseDataTypes(map: Map[String, String]): Map[String, DataType] = {
    map.map(x => (x._1, parseDataType(x._2)))
  }

  /** @return
    *   All supported data types.
    */
  def allTypes: Seq[String] = {
    Seq(
      STRING_TYPE,
      BINARY_TYPE,
      BOOLEAN_TYPE,
      DATE_TYPE,
      DOUBLE_TYPE,
      BYTE_TYPE,
      INTEGER_TYPE,
      LONG_TYPE,
      SHORT_TYPE
    )
  }
}
