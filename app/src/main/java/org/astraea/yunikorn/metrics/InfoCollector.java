package org.astraea.yunikorn.metrics;

import lombok.Getter;
import lombok.Setter;
import org.astraea.yunikorn.config.NodeSortingPolicy;
import org.astraea.yunikorn.metrics.Infos.Info;

import java.math.BigInteger;
@Setter @Getter
public class InfoCollector {
    public static final String ZERO = "0";
    private Info info ;
    public void execute(){
        var averageCore = new BigInteger(ZERO);
        info.getNodes()
                .stream()
                .forEach(node -> averageCore.add(node.getAvailable()
                        .get(NodeSortingPolicy.CORE_KEY)));
        averageCore.divide(BigInteger.valueOf(info.getNodes()
                .stream()
                .count()));
        var averageMemory = new BigInteger(ZERO);
        info.getNodes()
                .stream()
                .forEach(node -> averageMemory.add(node.getAvailable()
                        .get(NodeSortingPolicy.MEMORY_KEY)));
        averageMemory.divide(BigInteger.valueOf(info.getNodes()
                .stream()
                .count()));
        var maxCore = new BigInteger(ZERO);
        info.getNodes()
                .stream()
                .forEach(node -> maxCore.max(node.getAllocated()
                        .get(NodeSortingPolicy.CORE_KEY)));
        var maxMemory = new BigInteger(ZERO);
        info.getNodes()
                .stream()
                .forEach(node -> maxMemory.max(node.getAllocated()
                        .get(NodeSortingPolicy.MEMORY_KEY)));
        var minCore =new  BigInteger(info.getNodes()
                .get(0)
                .getAvailable()
                .get(NodeSortingPolicy.MEMORY_KEY)
                .toString());
        info.getNodes()
                .stream()
                .forEach(node -> minCore.max(node.getAllocated()
                        .get(NodeSortingPolicy.CORE_KEY)));
        var minMemory = new BigInteger(info.getNodes()
                .get(0)
                .getAvailable()
                .get(NodeSortingPolicy.MEMORY_KEY)
                .toString());
        info.getNodes()
                .stream()
                .forEach(node -> minMemory.max(node.getAllocated()
                        .get(NodeSortingPolicy.MEMORY_KEY)));
    }



}
