package com.jiang.spout;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author ajiang
 * @create by 18-6-6 下午11:08
 */
public class MySpout implements Scheme {

    private Logger log = LoggerFactory.getLogger(MySpout.class);

    @Override
    public List<Object> deserialize(byte[] bytes){

        String msg = new String(bytes);
        Values values = new Values();
        values.add(msg);
        System.out.println("spout msg :  = [" + msg + "]");
        log.info("spout msg : {}", msg);
        return values;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("msg");
    }
}
