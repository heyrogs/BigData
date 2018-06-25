package com.jiang.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * @author jiang
 * <p>
 *     日志练习
 * </p>
 * Create by 2018/5/17 14:47
 */
public class SogouResult {

    //1.环境初始化
   static final SparkConf conf = new SparkConf()
            .setMaster("local").setAppName("sogou data acount");

    public static void sougou(){

        JavaSparkContext sc = new JavaSparkContext(conf);
        //输入源->
        JavaRDD rdd = sc.textFile("hdfs://hadoop:9000/user/sogou/SogouT-Link.mini.mapping.txt")
                .flatMap(line
                        -> Arrays.asList(Pattern.compile("\t").matcher(line)).iterator());
        sc.close();

    }

    /**
     *
     *  广播的使用
     *
     */
    public static void broadcast(){
        JavaSparkContext sc = new JavaSparkContext(conf);
        //创建一个广播对象
        Broadcast<Integer> broadcast = sc.broadcast(1);
        int num = broadcast.value();
        System.out.println("----->" + num);
    }


    public static void main(String[] args) {


    }



}
