package com.jiang.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author jiang
 * <p>
 *     word count
 * </p>
 * Create by 18-5-22 下午8:45
 */
public class WordCountBySparkStream {

    /**
     *
     *  run app
     *
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException{
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("streamDataCount");
        JavaStreamingContext sc  = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        JavaReceiverInputDStream<String> lines
                = sc.socketTextStream("hadoop1",9999);

        JavaDStream<String> words = lines.flatMap(line
                -> Arrays.asList(line.split(",")).iterator());

        JavaPairDStream<String,Integer> wordCount =
                words.mapToPair(word -> new Tuple2<>(word,1))
                        .reduceByKey((v1,v2)->(v1 + v2));

        wordCount.print();
        sc.start();
        sc.awaitTermination();
        if(sc != null)sc.close();
    }



}
