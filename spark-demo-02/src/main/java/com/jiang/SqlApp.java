package com.jiang;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author jiang
 * <p>
 * Create by 2018/6/25 14:35
 */
public class SqlApp {

    static final SparkConf conf = new SparkConf();

    static
    {
        conf.setMaster("local");
        conf.setAppName("mySparkApplication");
    }

    public static void main(String [] args){



    }

    public static void execute(final JavaSparkContext jsc){

        


    }


}
