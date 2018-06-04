package com.jiang.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.jiang.Constant;

import java.util.Map;

/**
 * @author jiang
 * <p>
 * Create by 2018/6/1 15:41
 */
public class TestBolt02 implements IRichBolt {

    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
          String value = tuple.getStringByField(Constant.SPOUT_KEY01);
          System.out.println("bolt 2  -> value :  [" + value + "] , data : " + tuple.toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
