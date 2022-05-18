package org.astraea.admin;

/**
 * Kafka quota APIs are too incomprehensible to use, so we re-design quota APIs to builder pattern.
 */
public interface QuotaCreator {

  /** the valid quota for IP is connection rate only. */
  interface Ip {
    /**
     * @param value An int representing the upper bound of connections
     * @return this object
     */
    Ip connectionRate(int value);

    void create();
  }

  /**
   * the valid quota for IP are produce, consume and controller. However, we show only produce and
   * consume here
   */
  interface Client {
    /**
     * @param value A rate representing the upper bound (bytes/sec) for producer traffic
     * @return this object
     */
    Client produceRate(int value);

    /**
     * @param value A rate representing the upper bound (bytes/sec) for consumer traffic
     * @return this object
     */
    Client consumeRate(int value);

    void create();
  }

  /**
   * start to set quota for specify ip address
   *
   * @param ip to add quota
   * @return ip quota creator
   */
  Ip ip(String ip);

  /**
   * start to set quota for specify client id
   *
   * @param id to add quota
   * @return client id quota creator
   */
  Client clientId(String id);
}
