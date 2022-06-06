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
package org.astraea.app.concurrent;

@FunctionalInterface
public interface Executor extends AutoCloseable {

  /**
   * @return the state of this executor
   * @throws InterruptedException This is an expected exception if your executor needs to call
   *     blocking method. This exception is not printed to console.
   */
  State execute() throws InterruptedException;

  /** close this executor. */
  @Override
  default void close() {}

  /**
   * If this executor is in blocking mode, this method offers a way to wake up executor to close.
   */
  default void wakeup() {}
}
