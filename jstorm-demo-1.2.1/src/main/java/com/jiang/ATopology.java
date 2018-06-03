package com.jiang;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import com.jiang.bolt.CountBolt;
import com.jiang.spout.WordSpout;

import java.util.HashMap;
import java.util.Map;

import static com.jiang.Constant.*;

/**
 * @author ajiang
 * @create by 18-6-3 下午8:34
 */
public class ATopology {


    public static void main(String[] args) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new WordSpout());
        //表示接收SPOUT_ID的数据，并且以shuffle方式，
        builder.setBolt(BOLT_ID, new CountBolt()).shuffleGrouping(SPOUT_ID);

        //本地模式:本地提交
        /*LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        config.setNumWorkers(2);
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
        cluster.submitTopology("firstTopo", config, builder.createTopology());
        //一定要等待足够的时间，否则程序没来得及运行就已经结束了，程序启动需要消耗时间
        Thread.sleep(30000);
        cluster.killTopology("firstTopo");
        cluster.shutdown();*/


        //remote
        //获取spout的并发设置
        Map conf = new HashMap();
        //表示整个topology将使用几个worker
        conf.put(Config.TOPOLOGY_WORKERS, 2);
        //设置topolog模式为分布式，这样topology就可以放到JStorm集群上运行
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        StormSubmitter.submitTopology(TOPOLOGY_ID, conf, builder.createTopology());
    }


}
