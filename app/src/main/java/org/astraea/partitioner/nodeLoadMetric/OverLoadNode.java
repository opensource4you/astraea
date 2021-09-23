package org.astraea.partitioner.nodeLoadMetric;

import java.util.HashMap;
import java.util.Map;

public class OverLoadNode {
    private double standardDeviation = 0;
    private double avgBrokersMsgPerSec = 0;
    private int[] nodesID;
    private int nodeNum;
    private int mountCount;
    private HashMap<Integer, Double> eachBrokerMsgPerSec = new HashMap();

    OverLoadNode() {
        this.nodesID = getNodesID();
        this.nodeNum = getNodesID().length;
    }

    /**
     * Monitor and update the number of overloads of each node.
     */
    public void monitorOverLoad(HashMap<Integer, Integer> overLoadCount) {
        int ifOverLoad = 0;
        setBrokersMsgPerSec();
        setAvgBrokersMsgPerSec();
        standardDeviationImperative();
        for (Map.Entry<Integer,Double> entry : eachBrokerMsgPerSec.entrySet()){
            if (entry.getValue() > (avgBrokersMsgPerSec + standardDeviation)) {
                ifOverLoad = 1;
            }
            overLoadCount.put(entry.getKey(),setOverLoadCount(overLoadCount.get(entry.getKey()), mountCount%10, ifOverLoad));
        }
        this.mountCount = mountCount++;
    }

    /**
     *Use bit operations to record whether the node exceeds the load per second,the position of the number represents the recorded time.
     */
    public int setOverLoadCount(int overLoadCount, int roundCount,int ifOverLoad){
        int x = overLoadCount&1<<roundCount;
        if(x == ifOverLoad<<roundCount){
            return overLoadCount;
        }else {
            if (ifOverLoad!=0){
                return overLoadCount | 1<<roundCount;
            }
            else {
                return overLoadCount - (int)Math.pow(2, roundCount);
            }
        }
    }

    public void setBrokersMsgPerSec() {
        for (int nodeID : nodesID){
            eachBrokerMsgPerSec.put(nodeID, getEachBrokerMsgPerSec(nodeID));
        }
    }

    public void setAvgBrokersMsgPerSec() {
        double avg = 0;
        for (Map.Entry<Integer,Double> entry : eachBrokerMsgPerSec.entrySet()) {
            avg += entry.getValue();
        }
        this.avgBrokersMsgPerSec = avg/nodeNum;
    }

    public void standardDeviationImperative() {
        double variance = 0;
        for (Map.Entry<Integer,Double> entry : eachBrokerMsgPerSec.entrySet()) {
            variance += (entry.getValue() - avgBrokersMsgPerSec) * (entry.getValue() - avgBrokersMsgPerSec);
        }

        this.standardDeviation = Math.sqrt(variance / nodeNum);
    }

    //Only for test
    public void setEachBrokerMsgPerSec(HashMap<Integer, Double> hashMap) {
        this.eachBrokerMsgPerSec = hashMap;
    }

    //Only for test
    public double getStandardDeviation() {
        return this.standardDeviation;
    }

    //Only for test
    public double getAvgBrokersMsgPerSec() {
        return this.avgBrokersMsgPerSec;
    }

    //Only for test
    public void setMountCount(int i) {
        this.mountCount = i;
    }

    //TODO
    private double getEachBrokerMsgPerSec(int nodeID){
        return 0;
    }

    //TODO
    private int[] getNodesID() {
        return new int[] {0, 1, 2, 3};
    }
}
