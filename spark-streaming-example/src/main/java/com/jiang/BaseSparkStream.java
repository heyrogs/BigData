package com.jiang;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author jiang
 * <p>
 *     base on api
 * </p>
 * Create by 18-5-22 下午8:47
 */
public class BaseSparkStream {


    /**
     *
     * initializer spark configure data
     *
     * @return
     */
    protected SparkConf sparkConf(){


        return new SparkConf().setMaster("local[2]").setAppName("streamDataCount");
    }


    /**
     *
     * init stream context
     *
     * @return
     */
    protected JavaStreamingContext streamingContext(){


        return new JavaStreamingContext(sparkConf(), Durations.seconds(5));
    }

}
