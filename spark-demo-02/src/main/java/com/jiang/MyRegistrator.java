package com.jiang;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;

/**
 * @author jiang
 * <p>
 * Create by 2018/6/25 11:23
 */
public class MyRegistrator implements org.apache.spark.serializer.KryoRegistrator {

    @Override
    public void registerClasses(Kryo kryo) {
        //kryo.register(); //add register class
    }
}
