package com.jiang.stream;

import com.jiang.bean.Word;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

import static com.jiang.Constant.*;

/**
 * @author jiang
 * <p>
 * Create by 2018/6/12 09:54
 */
public class WordCountStreaming {

    public void countWord() {
        SparkConf conf = new SparkConf();
        //每五秒切一下数据
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(5));
        //可以遍历多个topic下的数据
        Set<String> topics = new HashSet<>(Arrays.asList(TOPIC));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", BROKER_LIST);
        kafkaParams.put("zookeeper.connect", ZK_SERVERS + ZK_ROOT);
        kafkaParams.put("group.id", GROUP_ID);
        kafkaParams.put("auto.offset.reset", "smallest");
        kafkaParams.put("enable.auto.commit", "false");

        //Receiver DirectKafkaStream
        JavaPairInputDStream<String, String> directKafkaStream =
                KafkaUtils.createDirectStream(
                        sc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        kafkaParams,
                        topics
                );

        //Integration word collection
        JavaDStream<Word> javaDStream = directKafkaStream.map(
                line -> {
                    String word = line.toString();
                    int len = word.length();
                    return new Word(word, len);
                });

        //like mapReducer operator
        JavaPairDStream<String, Integer> javaPairDStream =
                javaDStream.mapToPair(word -> new Tuple2<>(word.getName(), 1)).reduceByKey((v1, v2) -> (v1 + v2));

        //write result to db
        javaPairDStream.foreachRDD((javaPairRdd, time) -> {
            List<Tuple2<String, Integer>> tuple2s = javaPairRdd.collect();
            for (Tuple2<String, Integer> tuple2 : tuple2s) { //show elements
                String word = tuple2._1;
                Integer len = tuple2._2;
                System.out.println("word: " + word + ",len : " + len);
            }
        });

        //start running sparkStreaming
        sc.start();
        try {
            sc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        sc.close();
    }
}
