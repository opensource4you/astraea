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
package org.astraea.app.database;

import java.util.Collection;

public interface TableQuery {

  /**
   * Normally, catalog in database is server instance or database instance.
   *
   * @param catalog to search
   * @return this query
   */
  TableQuery catalog(String catalog);

  /**
   * Normally, schema in database is namespace.
   *
   * @param schema to search
   * @return this query
   */
  TableQuery schema(String schema);

  /**
   * @param tableName to search
   * @return this query
   */
  TableQuery tableName(String tableName);

  /**
   * @return the tables matched to this query.
   */
  Collection<TableInfo> run();
}
