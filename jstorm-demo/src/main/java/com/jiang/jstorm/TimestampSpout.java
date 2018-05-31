package com.jiang.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * @author jiang
 * <p>
 *     做一个很无聊的事情：给定一个时间戳，输出对应的问候语.
       规则是：时间戳的十位对应的数字对应不同的时间段，0-2代
       表早上，3代表中午，4-6代表下午，7-9代表晚上，分别输出
       早上，中午，下午和晚上。
 *  </p>
 * Create by 2018/5/29 15:50
 */
public class TimestampSpout implements IRichSpout {

    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;


    public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        Long now =  System.currentTimeMillis();
        Values tuple = new Values(now);
        System.out.println("spout:" + tuple);
        this.collector.emit(tuple);
        try {
            Thread.sleep(1000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    public void ack(Object o) {

    }

    public void fail(Object o) {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
