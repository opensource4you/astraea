package org.astraea.yunikorn.metrics.Infos;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.astraea.yunikorn.config.NodeSortingPolicy;
import org.json.JSONException;
import org.json.JSONObject;

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

  public Node(JSONObject json) {
    try {
      nodeID = json.get(NODEID_KEY).toString();
      hostName = json.get(HOSTNAME_KEY).toString();
      rackName = json.get(RACKNAME_KEY).toString();
      capacity.putAll(unmarshall(json.get(CAPACITY_KEY).toString()));
      allocated.putAll(unmarshall(json.get(ALLOCATED_KEY).toString()));
      occupied.putAll(unmarshall(json.get(OCCUPIED_KEY).toString()));
      available.putAll(unmarshall(json.get(AVAILABLE_KEY).toString()));
      utilized.putAll(unmarshall(json.get(UTILIZED_KEY).toString()));
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

  private Map<String, BigInteger> unmarshall(String str) {
    str = str.substring(1, str.length() - 1);
    Map<String, BigInteger> tmp = new HashMap<>();
    if (str.length() < 2) {
      tmp.put(NodeSortingPolicy.CORE_KEY, BigInteger.ZERO);
      tmp.put(NodeSortingPolicy.MEMORY_KEY, BigInteger.ZERO);
      return tmp;
    }
    var pairs = str.split(" ");
    for (int j = 0; j < pairs.length; j++) {
      var pair = pairs[j];
      var keyValue = pair.split(":");
      tmp.put(keyValue[0], new BigInteger(keyValue[1]));
    }
    return tmp;
  }
}
