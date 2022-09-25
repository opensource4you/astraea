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

import org.apache.spark.sql.Encoder
import org.junit.jupiter.api.Test

class SparkUtilsTest {
  @Test def csvToJSONTest() = {
    val spark = SparkUtils.createSpark("local[2]")

    import spark.implicits._
    case class Person(name: String, age: Long)
    val data =
      Seq(Person("Michael", 29), Person("Andy", 30), Person("Justin", 19))
    implicit val make: Encoder[Person] =
      org.apache.spark.sql.Encoders.kryo[Person]
    val df =
      spark.createDataset(data).map(x => (x.name, x.age)).toDF("name", "age")

    val json = SparkUtils.csvToJSON(df, Map("name" -> "string"))
    val iterator = (0 to 2).iterator
    json
      .collectAsList()
      .forEach(row => {
        val i = iterator.next()
        assert(row(0) equals data(i).name)
        assert(
          row(1) equals s"{\"name\":\"${data(i).name}\",\"age\":${data(i).age}}"
        )
        println(row(1))
      })
  }
}
