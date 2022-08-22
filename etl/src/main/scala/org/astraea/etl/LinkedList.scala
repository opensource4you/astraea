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

class LinkedList {
  private var head: Node = _

  def add(element: String): Unit = {
    val newNode = new Node(element)

    val it = tail(head)
    if (it == null) {
      head = newNode
    } else {
      it.next = newNode
    }
  }

  private def tail(head: Node) = {
    var it: Node = null

    it = head
    while (it != null && it.next != null) {
      it = it.next
    }

    it
  }

  def remove(element: String): Boolean = {
    var result = false
    var previousIt: Node = null
    var it: Node = head
    while (!result && it != null) {
      if (0 == element.compareTo(it.data)) {
        result = true
        unlink(previousIt, it)
      }
      previousIt = it
      it = it.next
    }

    result
  }

  private def unlink(previousIt: Node, currentIt: Node): Unit = {
    if (currentIt == head) {
      head = currentIt.next
    } else {
      previousIt.next = currentIt.next
    }
  }

  def size(): Int = {
    var size = 0

    var it = head
    while (it != null) {
      size = size + 1
      it = it.next
    }

    size
  }

  def get(idx: Int): String = {
    var index = idx
    var it = head
    while (index > 0 && it != null) {
      it = it.next
      index = index - 1
    }

    if (it == null) {
      throw new IndexOutOfBoundsException("Index is out of range")
    }

    it.data
  }

  private class Node(val data: String) {
    var next: Node = _
  }
}
