package org.astraea.app.consumer.experiment;

import java.time.Duration;

public class RebalanceTime {
    private int id;
    private Duration rebalanceTime;

    public RebalanceTime(Duration rebalanceTime, int id){
        this.id = id;
        this.rebalanceTime = rebalanceTime;
    }
    Duration rebalanceTime(){
        return rebalanceTime;
    }

    @Override
    public String toString() {
        return "RebalanceTime{" +
                "id=" + id +
                ", rebalanceTime=" + rebalanceTime.toMillis() + "ms" +
                '}';
    }
}
