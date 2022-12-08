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
package org.astraea.database;

public interface TableCreator {

  /**
   * @param name table name
   * @return this creator
   */
  TableCreator name(String name);

  /**
   * create a normal column
   *
   * @param name column name
   * @param type column type
   * @return this creator
   */
  TableCreator column(String name, String type);

  /**
   * create a primary key
   *
   * @param name primary key name
   * @param type primary key type
   * @return this creator
   */
  TableCreator primaryKey(String name, String type);

  void run();
}
