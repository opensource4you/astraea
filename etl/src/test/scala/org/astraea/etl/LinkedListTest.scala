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

import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions._

class LinkedListTest {
  @Test def testConstructor(): Unit = {
    val list = new LinkedList()
    assertEquals(0, list.size())
  }

  @Test def testAdd(): Unit = {
    val list = new LinkedList()

    list.add("one")
    assertEquals(1, list.size())
    assertEquals("one", list.get(0))

    list.add("two")
    assertEquals(2, list.size())
    assertEquals("two", list.get(1))
  }

  @Test def testRemove(): Unit = {
    val list = new LinkedList()

    list.add("one")
    list.add("two")
    assertTrue(list.remove("one"))

    assertEquals(1, list.size())
    assertEquals("two", list.get(0))

    assertTrue(list.remove("two"))
    assertEquals(0, list.size())
  }

  @Test def testRemoveMissing(): Unit = {
    val list = new LinkedList()

    list.add("one")
    list.add("two")
    assertFalse(list.remove("three"))
    assertEquals(2, list.size())
  }
}
