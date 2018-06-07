package com.jiang.sparkstream;

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
 *      监控本地文件变动情况
 * </p>
 * Create by 2018/5/22 15:04
 */
public class LocalSparkStreamLisening {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("translate net code");

        // 创建Streaming的上下文，包括Spark的配置和时间间隔，这里时间为间隔20秒
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(20));
        //指定监控的目录，在这里为D:\wordcount\output
        JavaDStream<String> lines = sc.textFileStream("D:\\wordcount\\input");
        // 对指定文件夹变化的数据进行单词统计并且打印
        JavaDStream<String> word = lines.flatMap(line
                -> Arrays.asList(line.split("\t")).iterator());
        JavaPairDStream<String,Integer> wordCount =
                word.mapToPair(words->new Tuple2<>(words,1))
                .reduceByKey((v1,v2)->(v1+v2));
        wordCount.print();
        sc.start();
        try {
            sc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            sc.close();
        }
    }
}
