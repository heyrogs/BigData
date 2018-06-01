package com.jiang.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.jiang.Constant;

import java.util.Map;

/**
 * @author jiang
 * <p>
 * Create by 2018/6/1 15:41
 */
public class TestBolt01 implements IRichBolt {

    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String value = tuple.getStringByField(Constant.SPOUT_KEY01);
        String str = tuple.getString(0);
        System.out.println("source type = [" + tuple.getSourceComponent() + "]");
        System.out.println(" hi = [" + tuple.getStringByField(Constant.SPOUT_KEY02) + "]");
        this.collector.emit(new Values(value));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
         outputFieldsDeclarer.declare(new Fields(Constant.SPOUT_KEY01));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
