package org.astraea.etl.KafkaAdmin

//import org.astraea.common.admin.Admin

import org.astraea.it.RequireBrokerCluster
import org.junit.jupiter.api.Test


class TopicCreatorTest extends RequireBrokerCluster{
    @Test def TopicCreatorTest(): Unit ={
//      Using(Admin.of(bootstrapServers())){
//      }
    }

    @Test def  IllegalArgumentTopicCreatorTest():Unit={

    }
}
