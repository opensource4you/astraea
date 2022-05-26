package org.astraea.database;

public interface DatabaseClient extends AutoCloseable {

  static Builder builder() {
    return new Builder();
  }

  /**
   * start to query tables
   *
   * @return Table Query
   */
  TableQuery query();

  /**
   * start to create new table
   *
   * @return Table Creator
   */
  TableCreator tableCreator();

  /** @param name to delete */
  void deleteTable(String name);

  @Override
  void close();
}
