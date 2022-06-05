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

  /** @return the tables matched to this query. */
  Collection<TableInfo> run();
}
