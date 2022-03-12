package org.astraea.yunikorn.metrics.Infos;
import org.astraea.yunikorn.config.NodeSortingPolicy;
import lombok.Getter;
import lombok.Setter;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
@Getter @Setter
public class Info {
    private List<Node> nodes = new ArrayList<>();
    private Map<String, BigInteger> averageSource = new HashMap<>();
    private Map<String, BigInteger> maxSource = new HashMap<>();
    private Map<String, BigInteger> minSource = new HashMap<>();
    private Map<String, BigInteger> SDSource = new HashMap<>();
    public Info(List<Node> nodes){

    }
}
