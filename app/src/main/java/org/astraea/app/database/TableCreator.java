package org.astraea.app.database;

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
