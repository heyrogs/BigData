package com.jiang.bolt;

import static com.jiang.Constant.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author ajiang
 * @create by 18-6-3 下午8:12
 */
public class CountBolt implements IRichBolt {

    private static final Long serialVersionUID = 1L;

    private OutputCollector collector;

    Map<String,Integer> map = new HashMap<>();

    private FileWriter writer;

    //这里仅仅是启动了一个文件写的定时线程，每2秒将结果写到文件中，并清空map.
    @Override
    public void prepare(Map map, TopologyContext topologyContext
            , OutputCollector outputCollector) {
        this.collector = outputCollector;

        ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);

        try {
            writer = new FileWriter(ROOT_DIR + "/src/main/resources/1.log");
        } catch (IOException e) {
            e.printStackTrace();
        }


        //open thread
        pool.scheduleAtFixedRate(()->{
            try {
                writer.write("\n\r");
                writer.write("***************************************");
                System.out.println("***************************************");
                //Map.Entry<String,Integer> entry = map.entrySet();
                Set<Map.Entry<String,Integer>> entries = map.keySet();
                for(Map.Entry<String,Integer> entry : entries){
                    writer.write(entry.getKey()+" : "+entry.getValue());
                    writer.write("\r\n");
                    System.out.println(entry.getKey()+" : "+entry.getValue());
                }
                writer.flush();
                map.clear();
            }catch (IOException e){
                e.printStackTrace();
            }
        },2000L,2000L,TimeUnit.MILLISECONDS);


    }

    @Override
    public void execute(Tuple tuple) {
        String msg  = tuple.getStringByField(FIELD);
        if(map.get(msg) == null){
            map.put(msg,1);
        }else {
            Integer count = map.get(msg);
            count+=1;
            map.put(msg,count);
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
