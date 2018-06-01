package com.jiang.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.jiang.Constant;

import java.util.Map;

/**
 * @author jiang
 * <p>
 * Create by 2018/6/1 15:35
 */
public class TestSpout implements IRichSpout {

    private static final long serialVersionUID = 1L;
    SpoutOutputCollector collector;

    //初始化数据结构
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        System.err.println("spout.open.map" + map );
        this.collector = spoutOutputCollector;
    }

    //发送数据源
    @Override
    public void nextTuple() {
        Values values = new Values();
        values.add("nihao");
        values.add("hello java");
        this.collector.emit(values);
        /*try{
            Thread.sleep(5000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }*/
    }

    //指定数据格式或指定发送bolt的ID
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
       outputFieldsDeclarer.declare(new Fields(Constant.SPOUT_KEY01,Constant.SPOUT_KEY02));
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
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
