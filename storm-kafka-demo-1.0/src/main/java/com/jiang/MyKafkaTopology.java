package com.jiang;

import com.jiang.bolt.MyKafkaBolt;
import com.jiang.spout.MyKafkaScheme;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.ArrayList;
import java.util.List;


/**
 * @author ajiang
 * @create by 18-6-5 下午9:20
 */
public class MyKafkaTopology {

    public static void main(String[] args) throws Exception {

        String topic = "hello_topic" ;
        ZkHosts zkHosts = new ZkHosts("ajiang:2180");
        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic,
                "",
                "MyTrack") ;
        List<String> zkServers = new ArrayList<String>() ;
        zkServers.add("ajiang");
        spoutConfig.zkServers = zkServers;
        spoutConfig.zkPort = 2180;
        spoutConfig.socketTimeoutMs = 60000;
        //spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme()) ;
        spoutConfig.scheme = new SchemeAsMultiScheme(new MyKafkaScheme());

        TopologyBuilder builder = new TopologyBuilder() ;
        builder.setSpout("spout", new KafkaSpout(spoutConfig) ,1) ;
        builder.setBolt("bolt1", new MyKafkaBolt(), 1).shuffleGrouping("spout") ;

        Config conf = new Config ();
        conf.setDebug(false) ;

        if (args.length > 0) {
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("mytopology", conf, builder.createTopology());
        }
    }
}
