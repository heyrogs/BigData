package com.jiang;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.jiang.bolt.MyBolt;
import com.jiang.spout.MySpout;
import scala.actors.threadpool.Arrays;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * @author ajiang
 * @create by 18-6-6 下午11:13
 */
public class MyTopology {


    public static void main(String[] args) throws Exception {

        String topic = "hello_topic";
        BrokerHosts brokerHosts = new ZkHosts("ajiang:2180");
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, "", "MyTrack2");
        spoutConfig.zkServers = Arrays.asList(new String[]{"ajiang"});
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
