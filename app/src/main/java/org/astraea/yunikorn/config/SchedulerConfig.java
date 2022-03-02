package org.astraea.yunikorn.config;

import java.util.List;


public class SchedulerConfig {

    private List<PartitionConfig> partitions;
    private String checksum;
    public List<PartitionConfig> getPartitions() {
        return partitions;
    }
    public String getChecksum(){
        return checksum;
    }
    public void setPartitions(List<PartitionConfig> partitions){
        this.partitions = partitions;
    }
    public void setChecksum(String checksun){
        this.checksum = checksun;
    }
}
