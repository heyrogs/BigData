package com.jiang.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * @author jiang
 * <p>
 * Create by 2018/5/29 16:00
 */
public class TestTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("TimeStampSpout", new TimestampSpout());
        builder.setBolt("GreetingBolt",new GreetingBolt()).shuffleGrouping("TimeStampSpout");
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        cluster.submitTopology("test",config,builder.createTopology());
    }


}
