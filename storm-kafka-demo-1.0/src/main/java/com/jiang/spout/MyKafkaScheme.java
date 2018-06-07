package com.jiang.spout;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author ajiang
 * @create by 18-6-6 下午10:33
 */
public class MyKafkaScheme  implements Scheme {

    @Override
    public List<Object> deserialize(ByteBuffer byteBuffer){

        String msg = "";
        try {
            byte [] bytes = byteBuffer.array();
            msg = new String(bytes,"UTF-8");
            System.out.println("scheme msg = [" + msg + "]");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        Values values = new Values();
        values.add(msg);
        return values;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("hello");
    }
}

