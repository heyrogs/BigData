package com.jiang.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.math.RandomUtils;
import static com.jiang.Constant.*;


import java.util.Map;

/**
 * @author ajiang
 * @create by 18-6-3 下午8:05
 */
public class WordSpout implements IRichSpout {

    private static final Long serialVersionUID = 1L;

    private String [] strs = {"one","two","three","four","five","six"};

    private SpoutOutputCollector collector;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        int index =RandomUtils.nextInt(strs.length);
        String str = strs[index];
        this.collector.emit(new Values(str));
        System.out.println("***************nextTuple() : "+strs[index]);
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(FIELD));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
