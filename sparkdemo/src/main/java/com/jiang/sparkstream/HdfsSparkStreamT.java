package com.jiang.sparkstream;

import lombok.extern.log4j.Log4j;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author jiang
 * <p>
 *     案例实战：监控hdfs目录，实现worldcount计算。
 * </p>
 * Create by 2018/5/21 15:31
 */
@Log4j
public class HdfsSparkStreamT {

    //check ping 存放数据文件夹
    final String CHECK_POINT_DIRECTORY = "hdfs://hadoop1:9000/lib/sparkstreaming/check_point";
    //spark streaming的监控文件夹
    final String LICENING_DATA_DIRRECTORY = "hdfs://hadoop1:9000/lib/sparkstreaming/data";
    //spark conf
    //final SparkConf conf = new SparkConf() .setMaster("spark://hadoop1:7077").setAppName("SparkStreamingOnHDFS");
    final SparkConf conf = new SparkConf() .setMaster("local[2]").setAppName("SparkStreamingOnHDFS");

    /**
     *
     * @param checkPointdir
     * @param conf
     * @return
     */
    public JavaStreamingContext context(final String checkPointdir
            ,final SparkConf conf){
        log.info("create JavaStreamingContext...");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(15));
        jsc.checkpoint(checkPointdir);
        return jsc;
    }

    public void run(){

        JavaStreamingContext jsc = context(CHECK_POINT_DIRECTORY,conf);
        //数据来源
        JavaDStream<String> lines = jsc.textFileStream(LICENING_DATA_DIRRECTORY);
        //编程处理
        JavaDStream<String> words = lines.flatMap(line
                -> Arrays.asList(line.split("\t")).iterator());

        //实现思路: 给每个单词添加标记1 -> 字典序排序 -> 分组 -> 同组别叠加
        JavaPairDStream<String,Integer> wordCount =
                words.mapToPair(word -> new Tuple2<>(word,1))
                .reduceByKey((v1,v2) -> (v1+v2));

        wordCount.print();
        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            jsc.close();
        }
    }


    public static void main(String[] args) {
        new HdfsSparkStreamT().run();
    }

}
