package org.astraea.yunikorn.metrics.Infos;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Node {
  public static final String NODEID_KEY = "nodeID";
  public static final String HOSTNAME_KEY = "hostName";
  public static final String RACKNAME_KEY = "rackName";
  public static final String CAPACITY_KEY = "capacity";
  public static final String ALLOCATED_KEY = "allocated";
  public static final String OCCUPIED_KEY = "occupied";
  public static final String AVAILABLE_KEY = "available";
  public static final String UTILIZED_KEY = "utilized";
  private String nodeID;
  private String hostName;
  private String rackName;
  private Map<String, BigInteger> capacity = new HashMap<>();
  private Map<String, BigInteger> allocated = new HashMap<>();
  private Map<String, BigInteger> occupied = new HashMap<>();
  private Map<String, BigInteger> available = new HashMap<>();
  private Map<String, BigInteger> utilized = new HashMap<>();

  public Node(
      String nodeID,
      String hostName,
      String rackName,
      Map<String, BigInteger> capacity,
      Map<String, BigInteger> available) {
    this.nodeID = nodeID;
    this.hostName = hostName;
    this.rackName = rackName;
    this.capacity = capacity;
    this.available = available;
  }
}
