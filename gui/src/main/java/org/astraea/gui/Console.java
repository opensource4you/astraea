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
package org.astraea.gui;

public interface Console {

  default void text(String text, Throwable e) {
    if (e != null) text(e);
    else text(text);
  }

  void text(String text);

  default void text(Throwable e) {
    if (e != null) text(Utils.toString(e));
  }

  void append(String text);

  void append(Throwable e);

  default void append(String text, Throwable e) {
    if (e != null) append(e);
    else append(text);
  }
}
