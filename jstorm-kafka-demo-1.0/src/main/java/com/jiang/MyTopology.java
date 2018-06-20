package com.jiang;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.jiang.bolt.MyBolt;
import com.jiang.spout.MySpout;
import scala.actors.threadpool.Arrays;
import storm.kafka.*;
import storm.kafka.trident.GlobalPartitionInformation;

import static com.jiang.Constant.*;

/**
 * @author ajiang
 * @create by 18-6-6 下午11:13
 */
public class MyTopology {


    public static void main(String[] args) throws Exception {

        BrokerHosts brokerHosts = new ZkHosts(BROKER_ZK_STR, BROKER_ZK_ROOT);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, TOPIC, ZK_ROOT, BROKER_ID);
        spoutConfig.zkServers = Arrays.asList(ZK_SERVERS.split(","));
        spoutConfig.forceFromStart = false;
        spoutConfig.zkPort = 2180;
        spoutConfig.socketTimeoutMs = 60000;
        spoutConfig.scheme = new SchemeAsMultiScheme(new MySpout());

        //topology builder
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("mySpout", new KafkaSpout(spoutConfig), 1);
        builder.setBolt("myBolt", new MyBolt(), 1).shuffleGrouping("mySpout");

        Config config = new Config();
        config.setDebug(false);
        if (args.length > 0) {
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            String topologyName = "myTopology";
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, config, builder.createTopology());
        }
    }
}
