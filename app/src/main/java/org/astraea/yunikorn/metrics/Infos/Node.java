package org.astraea.yunikorn.metrics.Infos;

import lombok.Getter;
import lombok.Setter;

import java.math.BigInteger;
import java.util.Map;
@Getter @Setter
public class Node {
    private String nodeID;
    private String hostName;
    private String rackName;
    private Map<String, BigInteger> capacity = Map.of();
    private Map<String, BigInteger> allocated = Map.of();
    private Map<String, BigInteger> occupied = Map.of();
    private Map<String, BigInteger> available = Map.of();
    private Map<String, BigInteger> utilized = Map.of();

}
